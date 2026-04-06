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

# =========================
# CONFIG
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lemulux")

app = FastAPI(title="lemulux-odoo")

IVA_RATE = 1.19
ML_DEFAULT_EMAIL = "boleta@lemulux.com"
TOKEN_REFRESH_INTERVAL = 5 * 60 * 60  # 5 horas
DB_RETRIES = 20
DB_RETRY_SECONDS = 3


# =========================
# HELPERS GENERALES
# =========================

def get_env(name: str, required: bool = True, default: str = "") -> str:
    value = os.getenv(name, default).strip()
    if required and not value:
        raise RuntimeError(f"Variable de entorno faltante: {name}")
    return value


def normalize_rut(rut: str) -> str:
    return rut.replace(".", "").replace("-", "").replace(" ", "").upper() if rut else ""


def db_url_from_env() -> str:
    db_url = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
    if not db_url:
        raise RuntimeError("Falta DATABASE_URL en variables de entorno")
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return db_url


# =========================
# BASE DE DATOS PostgreSQL
# =========================

def get_db():
    return psycopg2.connect(
        db_url_from_env(),
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10,
    )


def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """
            )
            cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS direccion TEXT")
        conn.commit()
    logger.info("✅ Tabla ventas verificada en PostgreSQL")


def wait_for_db():
    last_error = None
    for attempt in range(1, DB_RETRIES + 1):
        try:
            init_db()
            logger.info("✅ Conexión a PostgreSQL lista")
            return True
        except Exception as e:
            last_error = e
            logger.warning(
                f"⏳ PostgreSQL no disponible (intento {attempt}/{DB_RETRIES}): {e}"
            )
            time.sleep(DB_RETRY_SECONDS)
    raise RuntimeError(f"No se pudo inicializar PostgreSQL: {last_error}")


def save_venta(order: dict, billing: dict, tipo_sugerido: str,
               cliente: str, rut: str, giro: str, direccion: str):
    oid = str(order["id"])
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM ventas WHERE id = %s", (oid,))
            if cur.fetchone():
                logger.info(f"[{oid}] Venta ya existe en bandeja")
                return
            cur.execute(
                """
                INSERT INTO ventas
                    (id, cliente, rut, email, giro, direccion, tipo_sugerido, estado, order_json, billing_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s)
                """,
                (
                    oid,
                    cliente,
                    rut,
                    ML_DEFAULT_EMAIL,
                    giro,
                    direccion,
                    tipo_sugerido,
                    json.dumps(order),
                    json.dumps(billing),
                ),
            )
        conn.commit()
    logger.info(f"[{oid}] Guardada → tipo={tipo_sugerido} rut={rut or 'sin RUT'} cliente={cliente}")


def list_ventas(estado: Optional[str] = None) -> list:
    with get_db() as conn:
        with conn.cursor() as cur:
            if estado:
                cur.execute("SELECT * FROM ventas WHERE estado = %s ORDER BY creado_en DESC", (estado,))
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
    if not kwargs:
        return
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
# EXTRACCIÓN BILLING MLC
# =========================

def get_billing_info(billing_response: dict) -> dict:
    if "buyer" in billing_response:
        return billing_response["buyer"].get("billing_info", {}) or {}
    if "billing_info" in billing_response:
        return billing_response["billing_info"] or {}
    return billing_response


def extract_rut(billing_info: dict) -> str:
    identification = billing_info.get("identification") or {}
    number = identification.get("number", "")
    if number:
        return normalize_rut(str(number))

    for item in billing_info.get("additional_info") or []:
        if item.get("type") in ("DOC_NUMBER", "RUT"):
            return normalize_rut(str(item.get("value", "")))
    return ""


def extract_name(billing_info: dict, buyer: dict) -> str:
    name = billing_info.get("name", "").strip()
    last_name = billing_info.get("last_name", "").strip()
    if name and last_name:
        return f"{name} {last_name}"
    if name:
        return name
    return buyer.get("nickname") or buyer.get("first_name") or "Cliente ML"


def extract_activity(billing_info: dict) -> str:
    taxes = billing_info.get("taxes") or {}
    return taxes.get("economic_activity", "").strip()


def extract_direccion(billing_info: dict) -> str:
    address = billing_info.get("address") or {}
    if not address:
        return ""

    parts = []
    street = address.get("street_name", "")
    number = address.get("street_number", "")
    comment = address.get("comment", "")
    city = address.get("city_name", "")
    state = address.get("state", {}).get("name", "") if isinstance(address.get("state"), dict) else ""

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
    attributes = billing_info.get("attributes") or {}
    cust_type = attributes.get("cust_type", "").upper()
    if cust_type == "BU":
        return "Factura"
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
        res = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
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


def odoo_exec(ctx: OdooCtx, model: str, method: str, args: list, kwargs: dict | None = None) -> Any:
    return ctx.models.execute_kw(ctx.db, ctx.uid, ctx.password, model, method, args, kwargs or {})


def get_journal(ctx: OdooCtx, tipo: str) -> Optional[int]:
    if tipo == "Factura":
        ids = odoo_exec(ctx, "account.journal", "search", [[[
            "type", "=", "sale"], ["name", "=", "Facturas de cliente"]]], {"limit": 1})
    else:
        ids = odoo_exec(ctx, "account.journal", "search", [[[
            "type", "=", "sale"], ["name", "ilike", "Boleta"]]], {"limit": 1})
    return ids[0] if ids else None


def get_chile_country_id(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(ctx, "res.country", "search", [[["code", "=", "CL"]]], {"limit": 1})
    return ids[0] if ids else None


def get_rut_id_type(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(ctx, "l10n_latam.identification.type", "search", [[["name", "ilike", "RUT"]]], {"limit": 1})
    return ids[0] if ids else None


def get_tax_19(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "account.tax",
        "search",
        [[["type_tax_use", "=", "sale"], ["amount", "=", 19], ["active", "=", True]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut_norm = normalize_rut(rut)
    if not rut_norm:
        return None
    variantes = [rut_norm]
    if len(rut_norm) > 1:
        variantes.append(rut_norm[:-1] + "-" + rut_norm[-1])
    for variante in variantes:
        ids = odoo_exec(ctx, "res.partner", "search", [[["vat", "=", variante]]], {"limit": 1})
        if ids:
            return ids[0]
    return None


def get_or_create_partner(ctx: OdooCtx, nombre: str, rut: str, email: str,
                          giro: str, direccion: str, es_empresa: bool) -> int:
    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        current = odoo_exec(
            ctx,
            "res.partner",
            "read",
            [[partner_id], ["l10n_cl_sii_taxpayer_type", "email", "l10n_cl_dte_email", "country_id"]],
        )
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
    ids = odoo_exec(
        ctx,
        "account.move",
        "search",
        [[["ref", "=", f"ML-{order_id}"], ["state", "in", ["draft", "posted", "cancel"]]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def create_document(order: dict, billing_raw: dict, tipo: str, email: str, giro: str) -> int:
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

    partner_id = get_or_create_partner(ctx, nombre, rut, email, giro, direccion, es_empresa)

    journal_id = get_journal(ctx, tipo)
    doc_type_id = int(get_env("ODOO_DOC_TYPE_FACTURA_ID")) if tipo == "Factura" else int(get_env("ODOO_DOC_TYPE_BOLETA_ID"))
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
    wait_for_db()
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
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
        return {"status": "healthy", "db": bool(row and row.get("ok") == 1)}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "error": str(e)})


@app.get("/ventas")
def ventas(estado: Optional[str] = None):
    return {"items": list_ventas(estado)}


@app.get("/ventas/{oid}")
def venta_detalle(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    return venta


@app.patch("/ventas/{oid}")
def actualizar_venta(oid: str, payload: VentaUpdate):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    updates = {k: v for k, v in payload.dict().items() if v is not None}
    update_venta(oid, **updates)
    return {"ok": True, "id": oid, "updated": updates}


@app.post("/ventas/{oid}/autorizar")
def autorizar_venta(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    if venta.get("move_id"):
        return {"ok": True, "id": oid, "move_id": venta["move_id"], "message": "Documento ya existía"}

    try:
        order = json.loads(venta["order_json"])
        billing = json.loads(venta["billing_json"])
        tipo = venta.get("tipo_sugerido") or "Boleta"
        email = venta.get("email") or ML_DEFAULT_EMAIL
        giro = venta.get("giro") or ""

        move_id = create_document(order, billing, tipo, email, giro)
        update_venta(oid, estado="enviado", move_id=move_id, error=None, enviado_en="NOW()")
        return {"ok": True, "id": oid, "move_id": move_id}
    except Exception as e:
        update_venta(oid, estado="error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


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

    try:
        res = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
        res.raise_for_status()
        data = res.json()
        return HTMLResponse(f"<pre>{json.dumps(data, indent=2, ensure_ascii=False)}</pre>")
    except requests.HTTPError:
        detail = res.text if 'res' in locals() else 'Error desconocido'
        raise HTTPException(status_code=400, detail=detail)


@app.post("/ml/webhook")
async def ml_webhook(request: Request):
    body = await request.json()
    logger.info(f"Webhook recibido: {body}")

    resource = body.get("resource", "")
    topic = body.get("topic", "")

    if topic != "orders_v2" or not resource:
        return {"ok": True, "ignored": True}

    order_id = resource.strip("/").split("/")[-1]

    try:
        order = get_ml_order(order_id)
        if not order:
            return {"ok": False, "message": "Orden no encontrada en ML"}

        billing_raw = get_ml_billing_raw(order_id)
        billing_info = get_billing_info(billing_raw)

        rut = extract_rut(billing_info)
        cliente = extract_name(billing_info, order.get("buyer", {}))
        giro = extract_activity(billing_info)
        direccion = extract_direccion(billing_info)
        tipo_sugerido = detect_tipo(billing_info)

        save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion)
        return {"ok": True, "order_id": order_id, "tipo_sugerido": tipo_sugerido}
    except Exception as e:
        logger.exception(f"Error procesando webhook order_id={order_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ui", response_class=HTMLResponse)
def ui_bandeja():
    return """
    <!doctype html>
    <html lang='es'>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <title>Lemulux Odoo</title>
      <style>
        body { font-family: Arial, sans-serif; padding: 24px; background: #0f172a; color: #e2e8f0; }
        table { width: 100%; border-collapse: collapse; background: #111827; }
        th, td { border: 1px solid #334155; padding: 10px; text-align: left; }
        th { background: #1e293b; }
        button { background: #22c55e; border: 0; color: white; padding: 8px 12px; cursor: pointer; }
      </style>
    </head>
    <body>
      <h1>Bandeja de Ventas ML</h1>
      <p>Usa <code>/ventas</code> para consumir la API.</p>
    </body>
    </html>
    """
