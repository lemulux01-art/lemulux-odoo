from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
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
import re
from datetime import datetime
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
TOKEN_REFRESH_INTERVAL = 5 * 60 * 60
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


def safe_get(d: Any, *path, default=""):
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


# =========================
# BASE DE DATOS
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
            cur.execute("ALTER TABLE ventas ADD COLUMN IF NOT EXISTS giro TEXT")
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
            logger.warning(f"⏳ PostgreSQL no disponible ({attempt}/{DB_RETRIES}): {e}")
            time.sleep(DB_RETRY_SECONDS)
    raise RuntimeError(f"No se pudo inicializar PostgreSQL: {last_error}")


def save_venta(
    order: dict,
    billing: dict,
    tipo_sugerido: str,
    cliente: str,
    rut: str,
    giro: str,
    direccion: str,
    email: str = "",
):
    oid = str(order["id"])
    email_final = (email or ML_DEFAULT_EMAIL).strip()
    rut_final = normalize_rut(rut)
    direccion_final = (direccion or "").strip()
    cliente_final = (cliente or "Cliente ML").strip()
    giro_final = (giro or "").strip()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM ventas WHERE id = %s", (oid,))
            existing = cur.fetchone()

            if existing:
                cur.execute(
                    """
                    UPDATE ventas
                    SET cliente = %s,
                        rut = %s,
                        email = %s,
                        giro = %s,
                        direccion = %s,
                        tipo_sugerido = %s,
                        order_json = %s,
                        billing_json = %s
                    WHERE id = %s
                    """,
                    (
                        cliente_final,
                        rut_final,
                        email_final,
                        giro_final,
                        direccion_final,
                        tipo_sugerido,
                        json.dumps(order),
                        json.dumps(billing),
                        oid,
                    ),
                )
                conn.commit()
                logger.info(f"[{oid}] Venta actualizada")
                return

            cur.execute(
                """
                INSERT INTO ventas
                    (id, cliente, rut, email, giro, direccion, tipo_sugerido, estado, order_json, billing_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s)
                """,
                (
                    oid,
                    cliente_final,
                    rut_final,
                    email_final,
                    giro_final,
                    direccion_final,
                    tipo_sugerido,
                    json.dumps(order),
                    json.dumps(billing),
                ),
            )
        conn.commit()
    logger.info(f"[{oid}] Guardada → tipo={tipo_sugerido} rut={rut_final or 'sin RUT'} cliente={cliente_final}")


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
    cliente: Optional[str] = None
    rut: Optional[str] = None
    email: Optional[str] = None
    giro: Optional[str] = None
    direccion: Optional[str] = None
    tipo_sugerido: Optional[str] = None


# =========================
# EXTRACCIÓN ML
# =========================

def get_billing_info(billing_response: dict) -> dict:
    buyer_billing = safe_get(billing_response, "buyer", "billing_info", default={})
    if isinstance(buyer_billing, dict) and buyer_billing:
        return buyer_billing

    root_billing = billing_response.get("billing_info") or {}
    if isinstance(root_billing, dict) and root_billing:
        return root_billing

    return billing_response or {}


def extract_rut(billing_info: dict) -> str:
    number = safe_get(billing_info, "identification", "number", default="")
    if number:
        return normalize_rut(str(number))

    for item in billing_info.get("additional_info") or []:
        item_type = (item.get("type") or "").upper()
        if item_type in ("DOC_NUMBER", "RUT", "DOCUMENT_NUMBER"):
            return normalize_rut(str(item.get("value", "")))

    for key in ("rut", "vat", "doc_number", "document_number"):
        val = billing_info.get(key)
        if val:
            return normalize_rut(str(val))

    return ""


def extract_name(billing_info: dict, order: dict) -> str:
    for key in ("business_name", "company_name", "razon_social", "social_reason", "legal_name"):
        val = billing_info.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    taxes = billing_info.get("taxes") or {}
    for key in ("business_name", "company_name", "legal_name"):
        val = taxes.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    for item in billing_info.get("additional_info") or []:
        item_type = (item.get("type") or "").upper()
        if item_type in ("BUSINESS_NAME", "COMPANY_NAME", "RAZON_SOCIAL", "SOCIAL_REASON", "LEGAL_NAME"):
            val = item.get("value")
            if isinstance(val, str) and val.strip():
                return val.strip()

    name = (billing_info.get("name") or "").strip()
    last_name = (billing_info.get("last_name") or "").strip()
    if name and last_name:
        return f"{name} {last_name}"
    if name:
        return name

    buyer = order.get("buyer") or {}
    full_name = " ".join(
        x for x in [
            buyer.get("first_name", "").strip(),
            buyer.get("last_name", "").strip(),
        ] if x
    ).strip()
    if full_name:
        return full_name

    return buyer.get("nickname") or "Cliente ML"


def extract_activity(billing_info: dict, order: dict) -> str:
    taxes = billing_info.get("taxes") or {}
    for key in ("economic_activity", "activity", "giro", "business_activity"):
        val = taxes.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    for key in ("economic_activity", "activity", "giro", "business_activity"):
        val = billing_info.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    for item in billing_info.get("additional_info") or []:
        item_type = (item.get("type") or "").upper()
        if item_type in ("ECONOMIC_ACTIVITY", "ACTIVITY", "GIRO", "BUSINESS_ACTIVITY"):
            val = item.get("value")
            if isinstance(val, str) and val.strip():
                return val.strip()

    buyer = order.get("buyer") or {}
    for key in ("giro", "economic_activity", "activity"):
        val = buyer.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

    return ""


def extract_email(billing_info: dict, order: dict) -> str:
    email = (billing_info.get("email") or "").strip()
    if email:
        return email
    buyer = order.get("buyer") or {}
    buyer_email = (buyer.get("email") or "").strip()
    if buyer_email:
        return buyer_email
    return ML_DEFAULT_EMAIL


def format_address_dict(address: dict) -> str:
    if not isinstance(address, dict) or not address:
        return ""

    parts = []
    street = (
        address.get("street_name")
        or address.get("address_line")
        or address.get("line")
        or ""
    )
    number = address.get("street_number") or address.get("number") or ""
    comment = address.get("comment") or address.get("reference") or ""
    municipality = (
        address.get("municipality_name")
        or safe_get(address, "municipality", "name", default="")
    )
    city = (
        address.get("city_name")
        or safe_get(address, "city", "name", default="")
    )
    state = (
        address.get("state_name")
        or safe_get(address, "state", "name", default="")
    )
    zip_code = address.get("zip_code") or address.get("zipcode") or ""

    if street:
        parts.append(f"{street} {number}".strip())
    if comment:
        parts.append(comment)
    if municipality:
        parts.append(municipality)
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if zip_code:
        parts.append(zip_code)

    return ", ".join(filter(None, parts))


def flatten_strings(obj) -> list[str]:
    results = []

    def walk(x):
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str):
            s = " ".join(x.split()).strip()
            if s:
                results.append(s)

    walk(obj)
    return results


def looks_like_chilean_address(text: str) -> bool:
    if not text or len(text) < 10:
        return False

    t = " ".join(text.split()).strip()
    tl = t.lower()

    banned = [
        "rut",
        "giro",
        "actividad",
        "economic activity",
        "razon social",
        "razón social",
        "business name",
        "company name",
        "boleta",
        "factura",
        "consumidor final",
    ]
    if any(b in tl for b in banned):
        return False

    has_number = bool(re.search(r"\b\d{2,5}\b", t))
    has_street_word = bool(re.search(
        r"\b(calle|av\.?|avenida|pasaje|camino|ruta|general|manuel|los|las|el|la|encomenderos|montt|ohiggins|vicuña|providencia|apoquindo)\b",
        tl
    ))
    has_location_hint = bool(re.search(
        r"\b(santiago|las condes|providencia|maipu|maipú|ñuñoa|nunoa|coronel|biob[ií]o|metropolitana|valparaiso|valpara[ií]so|concepcion|concepción|temuco|antofagasta)\b",
        tl
    ))

    return has_number and (has_street_word or has_location_hint)


def score_address_candidate(text: str) -> int:
    t = text.lower()
    score = 0

    if re.search(r"\b\d{2,5}\b", t):
        score += 3
    if re.search(r"\b(of|oficina|depto|departamento|casa|piso|torre|edif|edificio)\b", t):
        score += 3
    if re.search(r"\b(santiago|las condes|providencia|coronel|biob[ií]o|metropolitana)\b", t):
        score += 3
    if re.search(r"\b(calle|av|avenida|pasaje|camino|ruta|general|manuel|encomenderos)\b", t):
        score += 2

    return score


def extract_direccion(order: dict, billing_info: dict, billing_raw: dict | None = None) -> str:
    # 1) ramas estructuradas
    candidates = [
        billing_info.get("address") or {},
        safe_get(order, "buyer", "address", default={}),
        safe_get(order, "buyer", "address_details", default={}),
        safe_get(order, "shipping", "receiver_address", default={}),
    ]

    for c in candidates:
        direccion = format_address_dict(c)
        if direccion:
            return direccion

    # 2) additional_info con dict o string
    for item in billing_info.get("additional_info") or []:
        value = item.get("value")
        if isinstance(value, dict):
            direccion = format_address_dict(value)
            if direccion:
                return direccion
        elif isinstance(value, str) and looks_like_chilean_address(value):
            return value.strip()

    # 3) búsqueda libre en todo el JSON
    blobs = []
    blobs.extend(flatten_strings(order))
    blobs.extend(flatten_strings(billing_info))
    if billing_raw:
        blobs.extend(flatten_strings(billing_raw))

    seen = set()
    uniq = []
    for s in blobs:
        if s not in seen:
            seen.add(s)
            uniq.append(s)

    filtered = [s for s in uniq if looks_like_chilean_address(s)]
    if filtered:
        filtered.sort(key=score_address_candidate, reverse=True)
        return filtered[0].strip()

    return ""


def detect_tipo(order: dict, billing_info: dict) -> str:
    cust_type = (safe_get(billing_info, "attributes", "cust_type", default="") or "").upper()
    if cust_type == "BU":
        return "Factura"
    if cust_type == "CO":
        return "Boleta"

    taxpayer_desc = (
        safe_get(billing_info, "taxes", "taxpayer_type", "description", default="") or ""
    ).strip().lower()

    if taxpayer_desc and any(x in taxpayer_desc for x in [
        "responsable", "empresa", "negocio", "iva", "jurídica", "juridica"
    ]):
        return "Factura"

    if extract_activity(billing_info, order):
        return "Factura"

    for key in ("business_name", "company_name", "razon_social", "social_reason", "legal_name"):
        val = billing_info.get(key)
        if isinstance(val, str) and val.strip():
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
        try:
            res = requests.get(url, headers=ml_headers(), timeout=30)
        except requests.RequestException as e:
            logger.error(f"Error de red en {url}: {e}")
            raise

        if res.status_code == 401:
            logger.warning(f"401 en {url}, renovando token...")
            if not refresh_ml_token():
                raise Exception("Token ML inválido y no se pudo renovar")
            continue

        if res.status_code == 429:
            wait = min(5 * (attempt + 1), 30)
            logger.warning(f"429 en {url}, reintento en {wait}s")
            time.sleep(wait)
            continue

        if res.status_code == 404:
            return {}

        res.raise_for_status()
        return res.json()

    raise Exception(f"ML devolvió demasiados errores para {url}")


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


def odoo_exec(ctx: OdooCtx, model: str, method: str, args: list, kwargs: dict = None) -> Any:
    return ctx.models.execute_kw(ctx.db, ctx.uid, ctx.password, model, method, args, kwargs or {})


def get_journal(ctx: OdooCtx, tipo: str) -> Optional[int]:
    if tipo == "Factura":
        ids = odoo_exec(
            ctx,
            "account.journal",
            "search",
            [[["type", "=", "sale"], ["name", "=", "Facturas de cliente"]]],
            {"limit": 1},
        )
    else:
        ids = odoo_exec(
            ctx,
            "account.journal",
            "search",
            [[["type", "=", "sale"], ["name", "ilike", "Boleta"]]],
            {"limit": 1},
        )
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


def get_or_create_partner(ctx: OdooCtx, nombre: str, rut: str, email: str, giro: str, direccion: str, es_empresa: bool) -> int:
    taxpayer_type = "1" if es_empresa else "4"
    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_id_type(ctx)

    base_vals = {
        "name": nombre,
        "vat": rut or False,
        "email": email,
        "l10n_cl_dte_email": email,
        "country_id": chile_id,
        "l10n_cl_sii_taxpayer_type": taxpayer_type,
        "company_type": "company" if es_empresa else "person",
        "is_company": es_empresa,
    }
    if direccion:
        base_vals["street"] = direccion
    if es_empresa and giro:
        base_vals["x_giro"] = giro
    if rut and rut_type_id:
        base_vals["l10n_latam_identification_type_id"] = rut_type_id

    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], base_vals])
        logger.info(f"Partner actualizado: id={partner_id}")
        return partner_id

    base_vals["customer_rank"] = 1
    partner_id = odoo_exec(ctx, "res.partner", "create", [base_vals])
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


def create_document(
    order: dict,
    billing_raw: dict,
    tipo: str,
    email: str,
    giro: str,
    cliente_override: Optional[str] = None,
    rut_override: Optional[str] = None,
    direccion_override: Optional[str] = None,
) -> int:
    ctx = odoo_connect()
    order_id = str(order["id"])

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return existing

    billing_info = get_billing_info(billing_raw)
    rut = normalize_rut(rut_override) if rut_override else extract_rut(billing_info)
    nombre = (cliente_override or extract_name(billing_info, order) or "Cliente ML").strip()
    direccion = (direccion_override or extract_direccion(order, billing_info, billing_raw) or "").strip()
    email = (email or ML_DEFAULT_EMAIL).strip()
    giro = (giro or "").strip()
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
# PROCESAMIENTO WEBHOOK
# =========================

def process_webhook_order(order_id: str):
    try:
        order = get_ml_order(order_id)
        if not order:
            logger.warning(f"[{order_id}] Orden no encontrada en ML")
            return

        if order.get("status") != "paid":
            logger.info(f"[{order_id}] Orden no pagada: {order.get('status')}")
            return

        billing_raw = get_ml_billing_raw(order_id)
        billing_info = get_billing_info(billing_raw)

        rut = extract_rut(billing_info)
        cliente = extract_name(billing_info, order)
        giro = extract_activity(billing_info, order)
        direccion = extract_direccion(order, billing_info, billing_raw)
        email = extract_email(billing_info, order)
        tipo_sugerido = detect_tipo(order, billing_info)

        save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion, email)
        logger.info(f"[{order_id}] ✅ Guardada/actualizada: {tipo_sugerido} dirección='{direccion or 'sin dirección'}' giro='{giro or 'sin giro'}'")
    except Exception as e:
        logger.error(f"[{order_id}] Error procesando webhook: {e}", exc_info=True)


def reprocesar_venta_desde_ml(order_id: str):
    order = get_ml_order(order_id)
    if not order:
        raise Exception("Orden no encontrada en ML")

    billing_raw = get_ml_billing_raw(order_id)
    billing_info = get_billing_info(billing_raw)

    rut = extract_rut(billing_info)
    cliente = extract_name(billing_info, order)
    giro = extract_activity(billing_info, order)
    direccion = extract_direccion(order, billing_info, billing_raw)
    email = extract_email(billing_info, order)
    tipo_sugerido = detect_tipo(order, billing_info)

    save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion, email)

    return {
        "id": str(order["id"]),
        "cliente": cliente,
        "rut": rut,
        "email": email,
        "giro": giro,
        "direccion": direccion,
        "tipo_sugerido": tipo_sugerido,
    }


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
# ENDPOINTS
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


@app.get("/debug/direccion/{oid}")
def debug_direccion(oid: str):
    order = get_ml_order(oid)
    billing_raw = get_ml_billing_raw(oid)
    billing_info = get_billing_info(billing_raw)

    return {
        "direccion_extraida": extract_direccion(order, billing_info, billing_raw),
        "buyer_address": safe_get(order, "buyer", "address", default={}),
        "buyer_address_details": safe_get(order, "buyer", "address_details", default={}),
        "shipping_receiver_address": safe_get(order, "shipping", "receiver_address", default={}),
        "billing_address": billing_info.get("address"),
        "billing_additional_info": billing_info.get("additional_info"),
        "top_candidates": sorted(
            [s for s in list(dict.fromkeys(flatten_strings(order) + flatten_strings(billing_raw))) if looks_like_chilean_address(s)],
            key=score_address_candidate,
            reverse=True
        )[:20]
    }


@app.post("/ml/webhook")
async def ml_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    topic = body.get("topic", "")
    resource = body.get("resource", "")
    if topic != "orders_v2" or not resource:
        return {"ok": True, "ignored": True}

    order_id = resource.strip("/").split("/")[-1]
    if not order_id:
        return {"ok": True}

    background_tasks.add_task(process_webhook_order, order_id)
    return {"ok": True, "order_id": order_id}


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
    res = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
    data = res.json()
    if res.status_code == 200:
        if data.get("access_token"):
            os.environ["ML_ACCESS_TOKEN"] = data["access_token"]
        if data.get("refresh_token"):
            os.environ["ML_REFRESH_TOKEN"] = data["refresh_token"]
    return JSONResponse(status_code=res.status_code, content={"status_code": res.status_code, "response": data})


@app.post("/ml/refresh-token")
def manual_refresh():
    return {"ok": refresh_ml_token()}


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
    if "rut" in updates:
        updates["rut"] = normalize_rut(updates["rut"])
    if "tipo_sugerido" in updates and updates["tipo_sugerido"] not in ["Boleta", "Factura"]:
        raise HTTPException(status_code=400, detail="tipo_sugerido debe ser Boleta o Factura")

    update_venta(oid, **updates)
    return {"ok": True, "id": oid, "updated": updates}


@app.post("/ventas/{oid}/reprocesar")
def reprocesar_venta(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    try:
        data = reprocesar_venta_desde_ml(str(oid))
        return {"ok": True, "id": oid, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/{oid}/autorizar")
def autorizar_venta(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    if venta.get("move_id") and venta.get("estado") == "enviado":
        return {"ok": True, "id": oid, "move_id": venta["move_id"], "message": "Ya enviada"}

    try:
        order = json.loads(venta["order_json"])
        billing = json.loads(venta["billing_json"])

        move_id = create_document(
            order=order,
            billing_raw=billing,
            tipo=venta.get("tipo_sugerido") or "Boleta",
            email=venta.get("email") or ML_DEFAULT_EMAIL,
            giro=venta.get("giro") or "",
            cliente_override=venta.get("cliente"),
            rut_override=venta.get("rut"),
            direccion_override=venta.get("direccion"),
        )
        update_venta(
            oid,
            estado="enviado",
            move_id=move_id,
            error=None,
            enviado_en=datetime.now(),
        )
        return {"ok": True, "id": oid, "move_id": move_id}
    except Exception as e:
        logger.error(f"[{oid}] Error al autorizar: {e}", exc_info=True)
        update_venta(oid, estado="error", error=str(e)[:500])
        raise HTTPException(status_code=500, detail=str(e))


# =========================
# DASHBOARD
# =========================

@app.get("/ui", response_class=HTMLResponse)
def ui_bandeja():
    return HTMLResponse(content="""
<!doctype html>
<html lang='es'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Lemulux | Bandeja de ventas</title>
  <style>
    :root {
      --bg: #0f172a; --panel: #111827; --panel2: #1f2937;
      --border: #334155; --text: #e5e7eb; --muted: #94a3b8;
      --ok: #22c55e; --warn: #f59e0b; --bad: #ef4444; --blue: #3b82f6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Arial, sans-serif; background: var(--bg); color: var(--text); }
    .wrap { max-width: 1300px; margin: 0 auto; padding: 24px; }
    .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
    .title { font-size: 26px; font-weight: 700; }
    .subtitle { color: var(--muted); margin-top: 4px; font-size: 14px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    button, select, input { border-radius: 8px; border: 1px solid var(--border); background: var(--panel); color: var(--text); padding: 9px 12px; font-size: 14px; }
    button { cursor: pointer; background: var(--blue); border: none; font-weight: 600; }
    button.secondary { background: var(--panel2); border: 1px solid var(--border); }
    button.success { background: var(--ok); color: #052e16; }
    button.warn { background: var(--warn); color: #3b2300; }
    .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 18px; }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 16px; }
    .card h3 { margin: 0 0 6px 0; font-size: 13px; color: var(--muted); font-weight: 600; }
    .card .value { font-size: 26px; font-weight: 700; }
    .toolbar { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; }
    .toolbar input { flex: 1; min-width: 220px; }
    table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--border); border-radius: 14px; overflow: hidden; }
    th, td { padding: 11px 13px; border-bottom: 1px solid var(--border); text-align: left; font-size: 13px; vertical-align: top; }
    th { background: #0b1220; color: var(--muted); font-weight: 700; font-size: 12px; text-transform: uppercase; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .badge { display: inline-block; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; }
    .badge-pendiente { background: rgba(245,158,11,0.15); color: #fbbf24; }
    .badge-enviado { background: rgba(34,197,94,0.15); color: #4ade80; }
    .badge-error { background: rgba(239,68,68,0.15); color: #f87171; }
    .badge-default { background: rgba(148,163,184,0.15); color: #cbd5e1; }
    .row-actions { display: flex; gap: 6px; flex-wrap: wrap; }
    .small { color: var(--muted); font-size: 12px; margin-top: 3px; }
    .empty { text-align: center; padding: 40px; color: var(--muted); background: var(--panel); border: 1px solid var(--border); border-radius: 14px; }
    .modal { position: fixed; inset: 0; background: rgba(2,6,23,0.8); display: none; align-items: center; justify-content: center; padding: 20px; z-index: 100; }
    .modal.open { display: flex; }
    .modal-card { width: min(760px, 100%); background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 24px; }
    .modal-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 16px; }
    .modal-grid .full { grid-column: 1 / -1; }
    label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 5px; font-weight: 600; }
    .modal-actions { display: flex; gap: 8px; margin-top: 18px; justify-content: flex-end; flex-wrap: wrap; }
    a.link { color: #93c5fd; text-decoration: none; font-size: 13px; }
    @media (max-width: 900px) { .grid { grid-template-columns: repeat(2, 1fr); } .modal-grid { grid-template-columns: 1fr; } }
    @media (max-width: 600px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<div class='wrap'>
  <div class='topbar'>
    <div>
      <div class='title'>🛒 Bandeja de ventas ML → Odoo</div>
      <div class='subtitle'>Revisa, edita, reprocesa y autoriza cada venta antes de crear el documento en Odoo.</div>
    </div>
    <div class='actions'>
      <button class='secondary' onclick='refreshData()'>↻ Actualizar</button>
      <a class='link' href='/health' target='_blank'>Health</a>
      <a class='link' href='/ventas' target='_blank'>API</a>
    </div>
  </div>

  <div class='grid'>
    <div class='card'><h3>Total</h3><div class='value' id='cTotal'>—</div></div>
    <div class='card'><h3>Pendientes</h3><div class='value' id='cPend'>—</div></div>
    <div class='card'><h3>Enviadas</h3><div class='value' id='cEnv'>—</div></div>
    <div class='card'><h3>Con error</h3><div class='value' id='cErr'>—</div></div>
  </div>

  <div class='toolbar'>
    <input id='searchInput' placeholder='Buscar por ID, cliente, RUT, email...' oninput='renderTable()'>
    <select id='statusFilter' onchange='renderTable()'>
      <option value=''>Todos los estados</option>
      <option value='pendiente'>Pendiente</option>
      <option value='enviado'>Enviado</option>
      <option value='error'>Error</option>
    </select>
    <button class='success' onclick='autorizarTodas()'>✓ Autorizar todas las pendientes</button>
  </div>

  <div id='tableWrap'><div class='empty'>Cargando...</div></div>
</div>

<div class='modal' id='editModal'>
  <div class='modal-card'>
    <div class='topbar' style='margin-bottom:0'>
      <div>
        <div class='title' style='font-size:20px'>✏️ Editar venta</div>
        <div class='subtitle' id='modalSub'></div>
      </div>
      <button class='secondary' onclick='closeModal()'>Cerrar</button>
    </div>
    <div class='modal-grid'>
      <div>
        <label>ID venta ML</label>
        <input id='editId' disabled>
      </div>
      <div>
        <label>Tipo documento</label>
        <select id='editTipo' onchange='toggleGiro()'>
          <option value='Boleta'>Boleta</option>
          <option value='Factura'>Factura</option>
        </select>
      </div>
      <div>
        <label>Email DTE</label>
        <input id='editEmail'>
      </div>
      <div>
        <label>RUT</label>
        <input id='editRut'>
      </div>
      <div class='full'>
        <label>Nombre cliente / razón social</label>
        <input id='editCliente'>
      </div>
      <div class='full'>
        <label>Dirección</label>
        <input id='editDireccion'>
      </div>
      <div class='full' id='giroGroup' style='display:none'>
        <label>Giro (solo factura)</label>
        <input id='editGiro'>
      </div>
    </div>
    <div class='modal-actions'>
      <button class='secondary' onclick='closeModal()'>Cancelar</button>
      <button class='warn' onclick='reprocesarActual()'>Reprocesar desde ML</button>
      <button onclick='saveEdit()'>Guardar</button>
      <button class='success' onclick='saveAndAuthorize()'>Guardar y autorizar</button>
    </div>
  </div>
</div>

<script>
let ventas = [];
let currentId = null;

function badge(estado) {
  const map = { pendiente: 'badge-pendiente', enviado: 'badge-enviado', error: 'badge-error' };
  return `<span class="badge ${map[estado] || 'badge-default'}">${estado}</span>`;
}

function safe(v) { return v == null || v === '' ? '—' : String(v); }

function copyId(id) { navigator.clipboard.writeText(String(id)); }

function updateStats(items) {
  document.getElementById('cTotal').textContent = items.length;
  document.getElementById('cPend').textContent = items.filter(v => v.estado === 'pendiente').length;
  document.getElementById('cEnv').textContent = items.filter(v => v.estado === 'enviado').length;
  document.getElementById('cErr').textContent = items.filter(v => v.estado === 'error').length;
}

function filteredVentas() {
  const q = document.getElementById('searchInput').value.trim().toLowerCase();
  const s = document.getElementById('statusFilter').value;
  return ventas.filter(v => {
    const okS = !s || v.estado === s;
    const okQ = !q || [v.id, v.cliente, v.rut, v.email, v.tipo_sugerido, v.direccion, v.giro].filter(Boolean).join(' ').toLowerCase().includes(q);
    return okS && okQ;
  });
}

function renderTable() {
  const items = filteredVentas();
  updateStats(ventas);
  const wrap = document.getElementById('tableWrap');
  if (!items.length) {
    wrap.innerHTML = "<div class='empty'>No hay ventas para mostrar.</div>";
    return;
  }

  wrap.innerHTML = `
    <table>
      <thead><tr>
        <th>Fecha / ID ML</th>
        <th>Cliente / Razón social</th>
        <th>RUT</th>
        <th>Dirección</th>
        <th>Tipo</th>
        <th>Estado</th>
        <th>Move ID</th>
        <th>Acciones</th>
      </tr></thead>
      <tbody>
        ${items.map(v => `
          <tr>
            <td>
              ${safe(v.creado_en ? new Date(v.creado_en).toLocaleString('es-CL') : null)}
              <div class='small'>${safe(v.id)}</div>
              <div class='small'><a href='#' class='link' onclick='copyId("${String(v.id).replace(/"/g, '&quot;')}"); return false;'>Copiar ID</a></div>
            </td>
            <td>
              <strong>${safe(v.cliente)}</strong>
              <div class='small'>${safe(v.email)}</div>
              ${v.giro ? `<div class='small'>${safe(v.giro)}</div>` : ''}
            </td>
            <td>${safe(v.rut)}</td>
            <td>${safe(v.direccion)}</td>
            <td>${safe(v.tipo_sugerido)}</td>
            <td>${badge(v.estado)}${v.error ? `<div class='small' style='color:#f87171;margin-top:4px'>${safe(v.error).substring(0,80)}</div>` : ''}</td>
            <td>${safe(v.move_id)}</td>
            <td>
              <div class='row-actions'>
                <button class='secondary' onclick='openEdit("${String(v.id).replace(/"/g, '&quot;')}")'>Editar</button>
                <button class='warn' onclick='reprocesar("${String(v.id).replace(/"/g, '&quot;')}")'>Reprocesar</button>
                ${v.estado !== 'enviado' ? `<button class='success' onclick='autorizar("${String(v.id).replace(/"/g, '&quot;')}")'>Autorizar</button>` : ''}
              </div>
            </td>
          </tr>
        `).join('')}
      </tbody>
    </table>`;
}

async function refreshData() {
  document.getElementById('tableWrap').innerHTML = "<div class='empty'>Cargando...</div>";
  try {
    const res = await fetch('/ventas');
    const data = await res.json();
    ventas = Array.isArray(data.items) ? data.items : [];
    renderTable();
  } catch (e) {
    document.getElementById('tableWrap').innerHTML = "<div class='empty'>Error cargando datos. Revisa /health</div>";
  }
}

async function autorizar(id) {
  if (!confirm(`¿Autorizar la venta ${id} y enviarla a Odoo?`)) return;
  const res = await fetch(`/ventas/${id}/autorizar`, { method: 'POST' });
  const data = await res.json().catch(() => ({}));
  if (res.ok) alert(`✅ Documento creado en Odoo: move_id=${data.move_id}`);
  else alert(`❌ Error: ${data.detail || 'desconocido'}`);
  await refreshData();
}

async function autorizarTodas() {
  const pendientes = ventas.filter(v => v.estado === 'pendiente');
  if (!pendientes.length) {
    alert('No hay ventas pendientes');
    return;
  }
  if (!confirm(`¿Autorizar las ${pendientes.length} ventas pendientes?`)) return;

  let ok = 0;
  let err = 0;
  for (const v of pendientes) {
    const r = await fetch(`/ventas/${v.id}/autorizar`, { method: 'POST' });
    if (r.ok) ok++; else err++;
  }
  alert(`✅ Autorizadas: ${ok} | ❌ Errores: ${err}`);
  await refreshData();
}

function toggleGiro() {
  document.getElementById('giroGroup').style.display =
    document.getElementById('editTipo').value === 'Factura' ? 'block' : 'none';
}

function openEdit(id) {
  const v = ventas.find(x => String(x.id) === String(id));
  if (!v) return;
  currentId = String(id);
  document.getElementById('modalSub').textContent = `Venta ${v.id}`;
  document.getElementById('editId').value = v.id || '';
  document.getElementById('editTipo').value = v.tipo_sugerido || 'Boleta';
  document.getElementById('editEmail').value = v.email || '';
  document.getElementById('editCliente').value = v.cliente || '';
  document.getElementById('editRut').value = v.rut || '';
  document.getElementById('editDireccion').value = v.direccion || '';
  document.getElementById('editGiro').value = v.giro || '';
  toggleGiro();
  document.getElementById('editModal').classList.add('open');
}

function closeModal() {
  currentId = null;
  document.getElementById('editModal').classList.remove('open');
}

async function saveEdit() {
  if (!currentId) return false;
  const tipo = document.getElementById('editTipo').value;
  const payload = {
    tipo_sugerido: tipo,
    email: document.getElementById('editEmail').value,
    cliente: document.getElementById('editCliente').value,
    rut: document.getElementById('editRut').value,
    direccion: document.getElementById('editDireccion').value,
    giro: tipo === 'Factura' ? document.getElementById('editGiro').value : '',
  };

  const res = await fetch(`/ventas/${currentId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    alert(data.detail || 'No se pudo guardar');
    return false;
  }

  await refreshData();
  closeModal();
  return true;
}

async function reprocesar(id) {
  const res = await fetch(`/ventas/${id}/reprocesar`, { method: 'POST' });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    alert(data.detail || 'No se pudo reprocesar');
    return;
  }
  alert('✅ Venta reprocesada desde Mercado Libre');
  await refreshData();
  if (currentId && String(currentId) === String(id)) openEdit(id);
}

async function reprocesarActual() {
  if (!currentId) return;
  await reprocesar(currentId);
}

async function saveAndAuthorize() {
  if (!currentId) return;
  const id = currentId;
  const ok = await saveEdit();
  if (!ok) return;
  await autorizar(id);
}

document.getElementById('editModal').addEventListener('click', e => {
  if (e.target.id === 'editModal') closeModal();
});

refreshData();
setInterval(refreshData, 30000);
</script>
</body>
</html>
""")
