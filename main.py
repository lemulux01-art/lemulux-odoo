from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
import os
import time
import threading
import logging
import requests
import xmlrpc.client
import psycopg2
import psycopg2.extras
import json
from typing import Optional, Any
from dataclasses import dataclass
from threading import Lock

# =========================
# CONFIG
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lemulux")

app = FastAPI()

IVA_RATE = 1.19
ML_DEFAULT_EMAIL = "boleta@lemulux.com"
TOKEN_REFRESH_INTERVAL = 5 * 60 * 60  # 5 horas


# =========================
# BASE DE DATOS PostgreSQL
# =========================

def get_db():
    db_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if not db_url:
        raise RuntimeError("Falta DATABASE_URL en variables de entorno")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(db_url, cursor_factory=psycopg2.extras.RealDictCursor)


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ventas (
                    id TEXT PRIMARY KEY,
                    cliente TEXT,
                    rut TEXT,
                    email TEXT,
                    giro TEXT,
                    direccion TEXT,
                    tipo_sugerido TEXT,
                    estado TEXT DEFAULT 'pendiente',
                    order_json TEXT,
                    billing_json TEXT,
                    move_id INTEGER,
                    error TEXT,
                    creado_en TIMESTAMP DEFAULT NOW(),
                    enviado_en TIMESTAMP
                )
            """)
            # Agregar columna direccion si no existe (para migraciones)
            try:
                cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS direccion TEXT")
            except Exception:
                pass
        conn.commit()
    logger.info("✅ Tabla ventas verificada en PostgreSQL")


def save_venta(order: dict, billing: dict, tipo_sugerido: str,
               cliente: str, rut: str, giro: str, direccion: str):
    oid = str(order["id"])
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM ventas WHERE id = %s", (oid,))
            if cur.fetchone():
                logger.info(f"[{oid}] Venta ya existe en bandeja")
                return
            cur.execute("""
                INSERT INTO ventas
                    (id, cliente, rut, email, giro, direccion, tipo_sugerido, estado, order_json, billing_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s)
            """, (oid, cliente, rut, ML_DEFAULT_EMAIL, giro, direccion,
                  tipo_sugerido, json.dumps(order), json.dumps(billing)))
        conn.commit()
    logger.info(f"[{oid}] Guardada → tipo={tipo_sugerido} rut={rut or 'sin RUT'} cliente={cliente}")


def list_ventas(estado: Optional[str] = None) -> list:
    with get_db() as conn:
        with conn.cursor() as cur:
            if estado:
                cur.execute(
                    "SELECT * FROM ventas WHERE estado = %s ORDER BY creado_en DESC", (estado,))
            else:
                cur.execute("SELECT * FROM ventas ORDER BY creado_en DESC")
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def get_venta(oid: str) -> Optional[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM ventas WHERE id = %s", (oid,))
            row = cur.fetchone()
    return dict(row) if row else None


def update_venta(oid: str, **kwargs):
    fields = ", ".join(f"{k} = %s" for k in kwargs)
    values = list(kwargs.values()) + [oid]
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE ventas SET {fields} WHERE id = %s", values)
        conn.commit()


# =========================
# MODELOS
# =========================

class VentaUpdate(BaseModel):
    email: Optional[str] = None
    giro: Optional[str] = None
    tipo_sugerido: Optional[str] = None


# =========================
# HELPERS
# =========================

def normalize_rut(rut: str) -> str:
    return rut.replace(".", "").replace("-", "").replace(" ", "").upper() if rut else ""


def get_env(name: str, required: bool = True) -> str:
    value = os.getenv(name, "").strip()
    if required and not value:
        raise RuntimeError(f"Variable de entorno faltante: {name}")
    return value


# =========================
# EXTRACCIÓN BILLING MLC
# =========================

def get_billing_info(billing_response: dict) -> dict:
    """
    La respuesta de /billing_info para MLC viene así:
    {
      "buyer": {
        "billing_info": { ... datos reales ... }
      }
    }
    Esta función extrae el billing_info real.
    """
    # Estructura nueva MLC: buyer.billing_info
    if "buyer" in billing_response:
        return billing_response["buyer"].get("billing_info", {}) or {}
    # Estructura antigua o directa
    if "billing_info" in billing_response:
        return billing_response["billing_info"] or {}
    # Ya es el billing_info directamente
    return billing_response


def extract_rut(billing_info: dict) -> str:
    """
    MLC estructura:
    - identification.number = RUT (persona o empresa)
    - identification.type = "RUT"
    """
    identification = billing_info.get("identification") or {}
    number = identification.get("number", "")
    if number:
        return normalize_rut(str(number))

    # Fallback: buscar en additional_info (estructura vieja)
    for item in billing_info.get("additional_info") or []:
        if item.get("type") in ("DOC_NUMBER", "RUT"):
            return normalize_rut(str(item.get("value", "")))

    return ""


def extract_name(billing_info: dict, buyer: dict) -> str:
    """
    - Empresa: billing_info.name = razón social
    - Persona: billing_info.name + billing_info.last_name
    """
    name = billing_info.get("name", "").strip()
    last_name = billing_info.get("last_name", "").strip()

    if name and last_name:
        return f"{name} {last_name}"
    if name:
        return name

    # Fallback al buyer de la orden
    return buyer.get("nickname") or buyer.get("first_name") or "Cliente ML"


def extract_activity(billing_info: dict) -> str:
    """
    Empresa MLC: billing_info.taxes.economic_activity
    """
    taxes = billing_info.get("taxes") or {}
    activity = taxes.get("economic_activity", "").strip()
    return activity


def extract_direccion(billing_info: dict) -> str:
    """
    MLC: billing_info.address
    """
    address = billing_info.get("address") or {}
    if not address:
        return ""

    parts = []
    street = address.get("street_name", "")
    number = address.get("street_number", "")
    comment = address.get("comment", "")
    city = address.get("city_name", "")
    state = ""
    if isinstance(address.get("state"), dict):
        state = address["state"].get("name", "")

    if street:
        parts.append(f"{street} {number}".strip())
    if comment:
        parts.append(comment)
    if city:
        parts.append(city)
    if state:
        parts.append(state)

    return ", ".join(filter(None, parts))


def detect_tipo(billing_info: dict) -> str:
    """
    MLC:
    - cust_type = "CO" → persona natural (boleta)
    - cust_type = "BU" → empresa (factura)
    También: si tiene taxes.economic_activity → factura
    """
    attributes = billing_info.get("attributes") or {}
    cust_type = attributes.get("cust_type", "").upper()

    if cust_type == "BU":
        return "Factura"

    # Tiene actividad económica → empresa
    if extract_activity(billing_info):
        return "Factura"

    return "Boleta"


# =========================
# MERCADO LIBRE API
# =========================

def ml_headers():
    return {"Authorization": f"Bearer {get_env('ML_ACCESS_TOKEN')}"}


def refresh_ml_token() -> bool:
    try:
        payload = {
            "grant_type": "refresh_token",
            "client_id": get_env("ML_CLIENT_ID"),
            "client_secret": get_env("ML_CLIENT_SECRET"),
            "refresh_token": get_env("ML_REFRESH_TOKEN"),
        }
        res = requests.post(
            "https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
        res.raise_for_status()
        data = res.json()
        if data.get("access_token"):
            os.environ["ML_ACCESS_TOKEN"] = data["access_token"]
        if data.get("refresh_token"):
            os.environ["ML_REFRESH_TOKEN"] = data["refresh_token"]
        logger.info("✅ Token ML renovado")
        return True
    except Exception as e:
        logger.error(f"❌ Error renovando token ML: {e}")
        return False


def schedule_token_refresh():
    while True:
        time.sleep(TOKEN_REFRESH_INTERVAL)
        logger.info("⏰ Renovación programada del token ML...")
        refresh_ml_token()


def ml_get(url: str) -> dict:
    for attempt in range(6):
        res = requests.get(url, headers=ml_headers(), timeout=30)
        if res.status_code == 401:
            logger.warning(f"401 en {url}, renovando token...")
            refresh_ml_token()
            res = requests.get(url, headers=ml_headers(), timeout=30)
        if res.status_code == 429:
            wait = min(5 * (attempt + 1), 30)
            logger.warning(f"429 en {url}, reintento en {wait}s")
            time.sleep(wait)
            continue
        if res.status_code == 404:
            return {}
        res.raise_for_status()
        return res.json()
    raise Exception(f"ML devolvió 429 demasiadas veces: {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_billing_raw(order_id: str) -> dict:
    """Retorna la respuesta cruda del billing_info de ML."""
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}/billing_info")


# =========================
# ODOO
# =========================

@dataclass
class OdooCtx:
    db: str
    uid: int
    password: str
    models: Any


def odoo_connect() -> OdooCtx:
    url = get_env("ODOO_URL")
    db = get_env("ODOO_DB")
    user = get_env("ODOO_USER")
    key = get_env("ODOO_API_KEY")
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, key, {})
    if not uid:
        raise Exception("No se pudo autenticar en Odoo")
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    return OdooCtx(db, uid, key, models)


def odoo_exec(ctx: OdooCtx, model: str, method: str, args: list, kwargs: dict = None) -> Any:
    return ctx.models.execute_kw(
        ctx.db, ctx.uid, ctx.password, model, method, args, kwargs or {})


def get_journal(ctx: OdooCtx, tipo: str) -> Optional[int]:
    if tipo == "Factura":
        ids = odoo_exec(ctx, "account.journal", "search",
            [[["type", "=", "sale"], ["name", "=", "Facturas de cliente"]]], {"limit": 1})
    else:
        ids = odoo_exec(ctx, "account.journal", "search",
            [[["type", "=", "sale"], ["name", "ilike", "Boleta"]]], {"limit": 1})
    return ids[0] if ids else None


def get_chile_country_id(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(ctx, "res.country", "search", [[["code", "=", "CL"]]], {"limit": 1})
    return ids[0] if ids else None


def get_rut_id_type(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(ctx, "l10n_latam.identification.type", "search",
        [[["name", "ilike", "RUT"]]], {"limit": 1})
    return ids[0] if ids else None


def get_tax_19(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(ctx, "account.tax", "search",
        [[["type_tax_use", "=", "sale"], ["amount", "=", 19], ["active", "=", True]]],
        {"limit": 1})
    return ids[0] if ids else None


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut_norm = normalize_rut(rut)
    if not rut_norm:
        return None
    variantes = [rut_norm]
    if len(rut_norm) > 1:
        variantes.append(rut_norm[:-1] + "-" + rut_norm[-1])
    for variante in variantes:
        ids = odoo_exec(ctx, "res.partner", "search",
            [[["vat", "=", variante]]], {"limit": 1})
        if ids:
            return ids[0]
    return None


def get_or_create_partner(ctx: OdooCtx, nombre: str, rut: str, email: str,
                           giro: str, direccion: str, es_empresa: bool) -> int:
    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        current = odoo_exec(ctx, "res.partner", "read",
            [[partner_id],
             ["l10n_cl_sii_taxpayer_type", "email", "l10n_cl_dte_email", "country_id"]])
        current = current[0] if current else {}
        vals = {}
        if not current.get("l10n_cl_sii_taxpayer_type"):
            vals["l10n_cl_sii_taxpayer_type"] = "1" if es_empresa else "4"
        if not current.get("email"):
            vals["email"] = email
        if not current.get("l10n_cl_dte_email"):
            vals["l10n_cl_dte_email"] = email
        if not current.get("country_id"):
            vals["country_id"] = get_chile_country_id(ctx)
        if vals:
            odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner existente: id={partner_id}")
        return partner_id

    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_id_type(ctx)

    vals = {
        "name": nombre,
        "vat": rut or False,
        "email": email,
        "l10n_cl_dte_email": email,
        "customer_rank": 1,
        "company_type": "company" if es_empresa else "person",
        "is_company": es_empresa,
        "country_id": chile_id,
        "l10n_cl_sii_taxpayer_type": "1" if es_empresa else "4",
    }

    if direccion:
        vals["street"] = direccion

    # Giro solo para empresas (factura)
    if es_empresa and giro:
        vals["x_giro"] = giro

    if rut and rut_type_id:
        vals["l10n_latam_identification_type_id"] = rut_type_id

    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado: id={partner_id} empresa={es_empresa}")
    return partner_id


# =========================
# CREACIÓN DOCUMENTO ODOO
# =========================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(ctx, "account.move", "search",
        [[["ref", "=", f"ML-{order_id}"],
          ["state", "in", ["draft", "posted", "cancel"]]]],
        {"limit": 1})
    return ids[0] if ids else None


def create_document(order: dict, billing_raw: dict, tipo: str,
                    email: str, giro: str) -> int:
    ctx = odoo_connect()
    order_id = str(order["id"])

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return existing

    billing_info = get_billing_info(billing_raw)
    rut = extract_rut(billing_info)
    nombre = extract_name(billing_info, order.get("buyer", {}))
    direccion = extract_direccion(billing_info)
    es_empresa = tipo == "Factura"

    partner_id = get_or_create_partner(
        ctx, nombre, rut, email, giro, direccion, es_empresa)

    journal_id = get_journal(ctx, tipo)
    doc_type_id = (
        int(get_env("ODOO_DOC_TYPE_FACTURA_ID")) if tipo == "Factura"
        else int(get_env("ODOO_DOC_TYPE_BOLETA_ID"))
    )
    tax_id = get_tax_19(ctx)

    lines = []
    for item in order.get("order_items", []):
        price = round(float(item["unit_price"]) / IVA_RATE, 2)
        line_vals = {
            "name": item["item"]["title"],
            "quantity": item["quantity"],
            "price_unit": price,
        }
        if tax_id:
            line_vals["tax_ids"] = [(6, 0, [tax_id])]
        lines.append((0, 0, line_vals))

    if not lines:
        raise Exception("La orden no tiene líneas")

    move_vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "ref": f"ML-{order_id}",
        "invoice_line_ids": lines,
        "l10n_latam_document_type_id": doc_type_id,
    }
    if journal_id:
        move_vals["journal_id"] = journal_id

    move_id = odoo_exec(ctx, "account.move", "create", [move_vals])
    odoo_exec(ctx, "account.move", "action_post", [[move_id]])
    logger.info(f"[{order_id}] ✅ Documento creado: move_id={move_id} tipo={tipo}")
    return move_id


# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def on_startup():
    init_db()
    t = threading.Thread(target=schedule_token_refresh, daemon=True)
    t.start()
    logger.info("⏰ Renovación automática de token ML iniciada (cada 5h)")


# =========================
# ENDPOINTS BASE
# =========================

@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")
    payload = {
        "grant_type": "authorization_code",
        "client_id": get_env("ML_CLIENT_ID"),
        "client_secret": get_env("ML_CLIENT_SECRET"),
        "code": code,
        "redirect_uri": get_env("ML_REDIRECT_URI"),
    }
    res = requests.post(
        "https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
    data = res.json()
    if res.status_code == 200:
        if data.get("access_token"):
            os.environ["ML_ACCESS_TOKEN"] = data["access_token"]
        if data.get("refresh_token"):
            os.environ["ML_REFRESH_TOKEN"] = data["refresh_token"]
        logger.info("✅ Tokens OAuth guardados")
    return JSONResponse(
        status_code=res.status_code,
        content={"status_code": res.status_code, "response": data})


@app.post("/ml/refresh-token")
def manual_refresh():
    ok = refresh_ml_token()
    return {"ok": ok}


# =========================
# WEBHOOK ML
# =========================

@app.post("/ml/webhook")
async def webhook(request: Request):
    data = await request.json()
    topic = data.get("topic", "")
    resource = data.get("resource", "")
    logger.info(f"Webhook: topic={topic} resource={resource}")

    if topic != "orders_v2":
        return {"ok": True, "message": "Topic ignorado"}

    order_id = resource.split("/")[-1]
    if not order_id:
        return {"ok": True}

    try:
        order = get_ml_order(order_id)
        if order.get("status") != "paid":
            return {"ok": True, "message": f"Orden no pagada: {order.get('status')}"}

        billing_raw = get_ml_billing_raw(order_id)
        billing_info = get_billing_info(billing_raw)
        buyer = order.get("buyer", {})

        rut = extract_rut(billing_info)
        nombre = extract_name(billing_info, buyer)
        tipo = detect_tipo(billing_info)
        giro = extract_activity(billing_info) if tipo == "Factura" else ""
        direccion = extract_direccion(billing_info)

        logger.info(
            f"[{order_id}] tipo={tipo} rut={rut or 'sin RUT'} "
            f"nombre={nombre} direccion={direccion or 'sin dirección'}"
        )
        save_venta(order, billing_raw, tipo, nombre, rut, giro, direccion)

    except Exception as e:
        logger.error(f"[{order_id}] Error procesando webhook: {e}", exc_info=True)

    return {"ok": True}


# =========================
# API BANDEJA
# =========================

@app.get("/bandeja")
def get_bandeja(estado: Optional[str] = None):
    return {"ventas": list_ventas(estado)}


@app.get("/bandeja/{id}")
def get_venta_detail(id: str):
    venta = get_venta(id)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    return venta


@app.patch("/bandeja/{id}")
def patch_venta(id: str, payload: VentaUpdate):
    venta = get_venta(id)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    updates = payload.dict(exclude_unset=True)
    if updates:
        update_venta(id, **updates)
    return {"ok": True}


@app.post("/bandeja/{id}/aprobar")
def aprobar_venta(id: str):
    venta = get_venta(id)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if venta["estado"] == "enviada":
        return {"ok": True, "message": "Ya fue enviada", "move_id": venta.get("move_id")}

    try:
        order = json.loads(venta["order_json"])
        billing_raw = json.loads(venta["billing_json"])

        move_id = create_document(
            order=order,
            billing_raw=billing_raw,
            tipo=venta["tipo_sugerido"],
            email=venta["email"],
            giro=venta["giro"] or "",
        )

        from datetime import datetime
        update_venta(id,
            estado="enviada",
            move_id=move_id,
            error=None,
            enviado_en=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return {"ok": True, "move_id": move_id}

    except Exception as e:
        logger.error(f"[{id}] Error al aprobar: {e}", exc_info=True)
        update_venta(id, estado="error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bandeja/{id}/rechazar")
def rechazar_venta(id: str):
    venta = get_venta(id)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    update_venta(id, estado="rechazada")
    return {"ok": True}


# =========================
# DEBUG (ver billing crudo de ML)
# =========================

@app.get("/debug/billing/{order_id}")
def debug_billing(order_id: str):
    billing_raw = get_ml_billing_raw(order_id)
    billing_info = get_billing_info(billing_raw)
    return {
        "raw": billing_raw,
        "billing_info_extraido": billing_info,
        "rut": extract_rut(billing_info),
        "nombre": extract_name(billing_info, {}),
        "tipo": detect_tipo(billing_info),
        "giro": extract_activity(billing_info),
        "direccion": extract_direccion(billing_info),
    }


# =========================
# DASHBOARD WEB
# =========================

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content="""
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Lemulux — Bandeja de Ventas</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f7fa; color: #333; }
  header { background: #6a1b9a; color: white; padding: 16px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.2rem; font-weight: 600; }
  .badge { background: rgba(255,255,255,0.2); border-radius: 20px; padding: 2px 10px; font-size: 0.8rem; }
  .container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
  .filters { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
  .filter-btn { padding: 6px 16px; border-radius: 20px; border: 1px solid #ddd; background: white; cursor: pointer; font-size: 0.85rem; transition: all 0.2s; }
  .filter-btn.active { background: #6a1b9a; color: white; border-color: #6a1b9a; }
  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .stat-card { background: white; border-radius: 10px; padding: 14px 16px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
  .stat-card .num { font-size: 1.6rem; font-weight: 700; }
  .stat-card .label { font-size: 0.75rem; color: #888; margin-top: 2px; }
  .pendiente .num { color: #e65100; }
  .enviada .num { color: #2e7d32; }
  .error-stat .num { color: #c62828; }
  table { width: 100%; background: white; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); border-collapse: collapse; overflow: hidden; }
  th { background: #f0f0f0; padding: 12px 14px; text-align: left; font-size: 0.78rem; color: #666; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
  td { padding: 11px 14px; border-top: 1px solid #f0f0f0; font-size: 0.85rem; vertical-align: middle; }
  tr:hover td { background: #fafafa; }
  .tag { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; }
  .tag.pendiente { background: #fff3e0; color: #e65100; }
  .tag.enviada { background: #e8f5e9; color: #2e7d32; }
  .tag.error { background: #ffebee; color: #c62828; }
  .tag.rechazada { background: #f5f5f5; color: #757575; }
  .tag.Factura { background: #e3f2fd; color: #1565c0; }
  .tag.Boleta { background: #f3e5f5; color: #6a1b9a; }
  .btn { padding: 5px 12px; border-radius: 6px; border: none; cursor: pointer; font-size: 0.8rem; font-weight: 600; transition: opacity 0.2s; }
  .btn:hover { opacity: 0.85; }
  .btn-green { background: #43a047; color: white; }
  .btn-red { background: #e53935; color: white; }
  .btn-edit { background: #f5f5f5; color: #333; border: 1px solid #ddd; }
  .actions { display: flex; gap: 6px; flex-wrap: wrap; }
  .empty { text-align: center; padding: 40px; color: #999; }
  .modal-bg { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.4); z-index: 100; align-items: center; justify-content: center; }
  .modal-bg.open { display: flex; }
  .modal { background: white; border-radius: 14px; padding: 24px; width: 500px; max-width: 95vw; }
  .modal h2 { font-size: 1rem; margin-bottom: 16px; }
  .form-group { margin-bottom: 14px; }
  .form-group label { display: block; font-size: 0.8rem; color: #666; margin-bottom: 4px; font-weight: 500; }
  .form-group input, .form-group select { width: 100%; padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 0.88rem; }
  .form-group .hint { font-size: 0.75rem; color: #999; margin-top: 3px; }
  .modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 16px; }
  .refresh-btn { margin-left: auto; padding: 6px 14px; background: white; border: 1px solid #ddd; border-radius: 6px; cursor: pointer; font-size: 0.85rem; }
  .approve-all-btn { padding: 6px 16px; background: #43a047; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 0.85rem; font-weight: 600; }
  small { color: #999; font-size: 0.78rem; }
</style>
</head>
<body>
<header>
  <div>🛒</div>
  <h1>Lemulux — Bandeja de Ventas ML</h1>
  <span class="badge" id="badge-total">...</span>
  <button class="refresh-btn" onclick="loadData()">↻ Actualizar</button>
</header>

<div class="container">
  <div class="stats">
    <div class="stat-card pendiente"><div class="num" id="cnt-pendiente">-</div><div class="label">Pendientes</div></div>
    <div class="stat-card enviada"><div class="num" id="cnt-enviada">-</div><div class="label">Enviadas a Odoo</div></div>
    <div class="stat-card error-stat"><div class="num" id="cnt-error">-</div><div class="label">Con error</div></div>
    <div class="stat-card"><div class="num" id="cnt-rechazada">-</div><div class="label">Rechazadas</div></div>
  </div>

  <div class="filters">
    <button class="filter-btn active" onclick="setFilter(null, this)">Todas</button>
    <button class="filter-btn" onclick="setFilter('pendiente', this)">Pendientes</button>
    <button class="filter-btn" onclick="setFilter('enviada', this)">Enviadas</button>
    <button class="filter-btn" onclick="setFilter('error', this)">Con error</button>
    <button class="filter-btn" onclick="setFilter('rechazada', this)">Rechazadas</button>
    <button class="approve-all-btn" onclick="aprobarTodas()">✓ Aprobar todas las pendientes</button>
  </div>

  <table>
    <thead>
      <tr>
        <th>Fecha</th>
        <th>Cliente / Dirección</th>
        <th>RUT</th>
        <th>Tipo</th>
        <th>Estado</th>
        <th>Acciones</th>
      </tr>
    </thead>
    <tbody id="tabla-body">
      <tr><td colspan="6" class="empty">Cargando...</td></tr>
    </tbody>
  </table>
</div>

<!-- Modal edición -->
<div class="modal-bg" id="modal-edit">
  <div class="modal">
    <h2>✏️ Editar venta antes de aprobar</h2>
    <input type="hidden" id="edit-id">
    <div class="form-group">
      <label>Email DTE</label>
      <input type="email" id="edit-email">
      <div class="hint">Se usará para enviar el documento tributario</div>
    </div>
    <div class="form-group">
      <label>Tipo de documento</label>
      <select id="edit-tipo" onchange="toggleGiro()">
        <option value="Boleta">Boleta</option>
        <option value="Factura">Factura</option>
      </select>
    </div>
    <div class="form-group" id="giro-group" style="display:none">
      <label>Giro / Actividad económica</label>
      <input type="text" id="edit-giro">
      <div class="hint">Requerido para emitir factura electrónica</div>
    </div>
    <div class="modal-actions">
      <button class="btn btn-edit" onclick="closeModal()">Cancelar</button>
      <button class="btn btn-edit" onclick="saveEdit()">Solo guardar</button>
      <button class="btn btn-green" onclick="saveAndApprove()">Guardar y aprobar</button>
    </div>
  </div>
</div>

<script>
let currentFilter = null;
let lastEditId = null;

function toggleGiro() {
  const tipo = document.getElementById('edit-tipo').value;
  document.getElementById('giro-group').style.display = tipo === 'Factura' ? 'block' : 'none';
}

async function loadData() {
  const url = currentFilter ? `/bandeja?estado=${currentFilter}` : '/bandeja';
  const res = await fetch(url);
  const data = await res.json();
  renderTable(data.ventas);

  const all = await fetch('/bandeja').then(r => r.json());
  const ventas = all.ventas;
  document.getElementById('badge-total').textContent = ventas.length + ' ventas';
  document.getElementById('cnt-pendiente').textContent = ventas.filter(v => v.estado === 'pendiente').length;
  document.getElementById('cnt-enviada').textContent = ventas.filter(v => v.estado === 'enviada').length;
  document.getElementById('cnt-error').textContent = ventas.filter(v => v.estado === 'error').length;
  document.getElementById('cnt-rechazada').textContent = ventas.filter(v => v.estado === 'rechazada').length;
}

function setFilter(estado, btn) {
  currentFilter = estado;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadData();
}

function renderTable(ventas) {
  const tbody = document.getElementById('tabla-body');
  if (!ventas || !ventas.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">No hay ventas en esta categoría</td></tr>';
    return;
  }
  tbody.innerHTML = ventas.map(v => `
    <tr>
      <td>${v.creado_en ? new Date(v.creado_en).toLocaleString('es-CL') : '-'}</td>
      <td>
        <strong>${v.cliente}</strong>
        ${v.direccion ? `<br><small>📍 ${v.direccion}</small>` : ''}
        <br><small>✉ ${v.email}</small>
      </td>
      <td>${v.rut || '<span style="color:#bbb">sin RUT</span>'}</td>
      <td><span class="tag ${v.tipo_sugerido}">${v.tipo_sugerido}</span></td>
      <td>
        <span class="tag ${v.estado}">${v.estado}</span>
        ${v.move_id ? `<br><small>Odoo #${v.move_id}</small>` : ''}
        ${v.error ? `<br><small style="color:#c62828" title="${v.error}">⚠ Error</small>` : ''}
      </td>
      <td>
        <div class="actions">
          ${v.estado === 'pendiente' || v.estado === 'error' ? `
            <button class="btn btn-edit" onclick="openEdit('${v.id}', '${(v.email||'').replace(/'/g,"\\'")}', '${(v.giro||'').replace(/'/g,"\\'")}', '${v.tipo_sugerido}')">Editar</button>
            <button class="btn btn-green" onclick="aprobar('${v.id}')">✓ Aprobar</button>
            <button class="btn btn-red" onclick="rechazar('${v.id}')">✗</button>
          ` : v.estado === 'enviada' ? `<span style="color:#888;font-size:0.8rem">✓ Listo</span>` : '-'}
        </div>
      </td>
    </tr>
  `).join('');
}

async function aprobar(id) {
  if (!confirm('¿Enviar esta venta a Odoo?')) return;
  const res = await fetch(`/bandeja/${id}/aprobar`, { method: 'POST' });
  const data = await res.json();
  if (data.ok) { alert(`✅ Documento creado en Odoo (move_id: ${data.move_id})`); loadData(); }
  else alert('❌ Error: ' + (data.detail || 'desconocido'));
}

async function aprobarTodas() {
  const res = await fetch('/bandeja?estado=pendiente');
  const data = await res.json();
  const pendientes = data.ventas || [];
  if (!pendientes.length) { alert('No hay ventas pendientes'); return; }
  if (!confirm(`¿Aprobar todas las ${pendientes.length} ventas pendientes?`)) return;

  let ok = 0, errores = 0;
  for (const v of pendientes) {
    const r = await fetch(`/bandeja/${v.id}/aprobar`, { method: 'POST' });
    const d = await r.json();
    if (d.ok) ok++; else errores++;
  }
  alert(`✅ Aprobadas: ${ok} | ❌ Errores: ${errores}`);
  loadData();
}

async function rechazar(id) {
  if (!confirm('¿Rechazar esta venta?')) return;
  await fetch(`/bandeja/${id}/rechazar`, { method: 'POST' });
  loadData();
}

function openEdit(id, email, giro, tipo) {
  lastEditId = id;
  document.getElementById('edit-id').value = id;
  document.getElementById('edit-email').value = email;
  document.getElementById('edit-giro').value = giro;
  document.getElementById('edit-tipo').value = tipo;
  toggleGiro();
  document.getElementById('modal-edit').classList.add('open');
}

function closeModal() {
  document.getElementById('modal-edit').classList.remove('open');
}

async function saveEdit() {
  const id = document.getElementById('edit-id').value;
  const tipo = document.getElementById('edit-tipo').value;
  await fetch(`/bandeja/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: document.getElementById('edit-email').value,
      giro: tipo === 'Factura' ? document.getElementById('edit-giro').value : '',
      tipo_sugerido: tipo,
    }),
  });
  closeModal();
  loadData();
}

async function saveAndApprove() {
  const id = document.getElementById('edit-id').value;
  const tipo = document.getElementById('edit-tipo').value;
  await fetch(`/bandeja/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: document.getElementById('edit-email').value,
      giro: tipo === 'Factura' ? document.getElementById('edit-giro').value : '',
      tipo_sugerido: tipo,
    }),
  });
  closeModal();
  setTimeout(() => aprobar(id), 400);
}

loadData();
setInterval(loadData, 30000);
</script>
</body>
</html>
""")
