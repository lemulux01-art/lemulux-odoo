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
DEFAULT_BOLETA_ACTIVITY = "(boleta)"
TOKEN_REFRESH_INTERVAL = 5 * 60 * 60
DB_RETRIES = 20
DB_RETRY_SECONDS = 3

# Confirmados en tu Odoo
ODOO_JOURNAL_FACTURA_ID = 10
ODOO_JOURNAL_BOLETA_ID = 21
ODOO_DOC_TYPE_FACTURA_ID = 1   # (33) Factura Electrónica
ODOO_DOC_TYPE_BOLETA_ID = 5    # (39) Boleta Electrónica
ODOO_PAYMENT_TERM_CONTADO_ID = 1

ODOO_STANDARD_NARRATION = (
    '<p>Para futuras compras:&nbsp;</p>'
    '<p><a href="https://lemulux.com/">https://lemulux.com/</a></p>'
    '<p>ventas@lemulux.com</p>'
)

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
                    partner_id INTEGER,
                    error TEXT,
                    creado_en TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'America/Santiago'),
                    enviado_en TIMESTAMP
                )
                """
            )
            for stmt in [
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS direccion TEXT",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS giro TEXT",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS partner_id INTEGER",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS ciudad TEXT",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS region TEXT",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS estado_envio TEXT DEFAULT 'paid'",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS pack_id TEXT",
            ]:
                cur.execute(stmt)
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
    ciudad: str = "",
    region: str = "",
    pack_id: str = "",
    order_items_override: list = None,
):
    # Si hay pack_id, usar ese como ID de la venta para consolidar
    oid = str(pack_id) if pack_id else str(order["id"])
    # Permitir pasar items consolidados de múltiples órdenes
    if order_items_override is not None:
        order = {**order, "order_items": order_items_override}
    email_final = (email or ML_DEFAULT_EMAIL).strip()
    rut_final = normalize_rut(rut)
    direccion_final = (direccion or "").strip()
    cliente_final = (cliente or "Cliente ML").strip()
    giro_final = (giro or "").strip()
    ciudad_final = (ciudad or "").strip()
    region_final = (region or "").strip()

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, estado, move_id FROM ventas WHERE id = %s", (oid,))
            existing = cur.fetchone()

            if existing:
                # Solo actualizar datos del webhook si la venta está pendiente
                # Si ya fue enviada o tiene datos editados, solo actualizar el JSON de la orden
                if existing["estado"] in ("enviado", "error"):
                    # Solo actualizar el JSON por si acaso, sin tocar datos editados
                    cur.execute(
                        "UPDATE ventas SET order_json = %s, billing_json = %s WHERE id = %s",
                        (json.dumps(order, ensure_ascii=False),
                         json.dumps(billing, ensure_ascii=False), oid),
                    )
                else:
                    # Pendiente: actualizar datos pero respetar ediciones manuales
                    cur.execute(
                        """
                        UPDATE ventas
                        SET cliente = COALESCE(NULLIF(cliente, 'Cliente ML'), %s, cliente),
                            rut = CASE WHEN (rut IS NULL OR rut = '') THEN %s ELSE rut END,
                            email = CASE WHEN (email IS NULL OR email = '') THEN %s ELSE email END,
                            direccion = CASE WHEN (direccion IS NULL OR direccion = '') THEN %s ELSE direccion END,
                            ciudad = CASE WHEN (ciudad IS NULL OR ciudad = '') THEN %s ELSE ciudad END,
                            region = CASE WHEN (region IS NULL OR region = '') THEN %s ELSE region END,
                            tipo_sugerido = %s,
                            order_json = %s,
                            billing_json = %s,
                            giro = CASE
                                WHEN (giro IS NULL OR giro = '' OR giro = '(boleta)') THEN %s
                                ELSE giro
                            END
                        WHERE id = %s
                        """,
                        (
                            cliente_final,
                            rut_final,
                            email_final,
                            direccion_final,
                            ciudad_final,
                            region_final,
                            tipo_sugerido,
                            json.dumps(order, ensure_ascii=False),
                            json.dumps(billing, ensure_ascii=False),
                            giro_final,
                            oid,
                        ),
                    )
                conn.commit()
                logger.info(f"[{oid}] Venta actualizada (estado={existing['estado']})")
                return

            cur.execute(
                """
                INSERT INTO ventas
                    (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region, tipo_sugerido, estado, estado_envio, order_json, billing_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s)
                """,
                (
                    oid,
                    pack_id or None,
                    cliente_final,
                    rut_final,
                    email_final,
                    giro_final,
                    direccion_final,
                    ciudad_final,
                    region_final,
                    tipo_sugerido,
                    order.get("status", "paid"),
                    json.dumps(order, ensure_ascii=False),
                    json.dumps(billing, ensure_ascii=False),
                ),
            )
        conn.commit()
    logger.info(f"[{oid}] Guardada → tipo={tipo_sugerido} rut={rut_final or 'sin RUT'} cliente={cliente_final}")


def list_ventas(estado: Optional[str] = None) -> list:
    # Excluir order_json y billing_json del listado — son grandes y solo se necesitan al autorizar
    cols = "id, cliente, rut, email, giro, direccion, ciudad, region, tipo_sugerido, estado, estado_envio, pack_id, move_id, partner_id, error, creado_en, enviado_en"
    with get_db() as conn:
        with conn.cursor() as cur:
            if estado:
                cur.execute(f"SELECT {cols} FROM ventas WHERE estado = %s ORDER BY creado_en DESC", (estado,))
            else:
                cur.execute(f"SELECT {cols} FROM ventas ORDER BY creado_en DESC")
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
    ciudad: Optional[str] = None
    region: Optional[str] = None
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


# Mapeo STATE_CODE ML → nombre región Odoo Chile
ML_STATE_CODE_TO_REGION = {
    "CL-AI": "Aysén del Gral. Carlos Ibáñez del Campo",
    "CL-AN": "Antofagasta",
    "CL-AP": "Arica y Parinacota",
    "CL-AR": "de la Araucania",
    "CL-AT": "Atacama",
    "CL-BI": "del BíoBio",
    "CL-CO": "Coquimbo",
    "CL-LI": "del Libertador Gral. Bernardo O'Higgins",
    "CL-LL": "de los Lagos",
    "CL-LR": "Los Ríos",
    "CL-MA": "Magallanes",
    "CL-ML": "del Maule",
    "CL-NB": "del Ñuble",
    "CL-RM": "Metropolitana",
    "CL-TA": "Tarapacá",
    "CL-VS": "Valparaíso",
}


def extract_from_additional_info(billing_info: dict) -> dict:
    """
    Extrae campos clave desde additional_info de MLC.
    Retorna dict con: street, number, neighborhood, city, state_name, state_code, zip_code
    """
    fields = {}
    for item in billing_info.get("additional_info") or []:
        t = (item.get("type") or "").upper()
        v = item.get("value")
        if isinstance(v, str) and v.strip():
            fields[t] = v.strip()
    return fields


def extract_direccion_from_additional_info(billing_info: dict) -> str:
    """
    Construye la dirección desde additional_info de MLC.
    Estructura real MLC: STREET_NAME, STREET_NUMBER, CITY_NAME, STATE_NAME, NEIGHBORHOOD
    """
    fields = extract_from_additional_info(billing_info)
    if not fields:
        return ""

    parts = []
    street = fields.get("STREET_NAME", "")
    number = fields.get("STREET_NUMBER", "")
    neighborhood = fields.get("NEIGHBORHOOD", "")
    city = fields.get("CITY_NAME", "")
    state = fields.get("STATE_NAME", "")

    if street:
        parts.append(f"{street} {number}".strip())
    if neighborhood and neighborhood.lower() != city.lower():
        parts.append(neighborhood)
    if city:
        parts.append(city)
    if state:
        parts.append(state)

    return ", ".join(filter(None, parts))


def extract_ciudad_from_billing(billing_info: dict) -> str:
    """Extrae la ciudad/comuna desde billing_info."""
    fields = extract_from_additional_info(billing_info)
    return (
        fields.get("CITY_NAME")
        or fields.get("NEIGHBORHOOD")
        or safe_get(billing_info, "address", "city_name", default="")
        or ""
    )


def extract_region_from_billing(billing_info: dict) -> str:
    """Extrae la región desde billing_info, mapeando STATE_CODE al nombre Odoo."""
    fields = extract_from_additional_info(billing_info)

    # Preferir mapeo por código (más preciso)
    state_code = fields.get("STATE_CODE", "").upper()
    if state_code and state_code in ML_STATE_CODE_TO_REGION:
        return ML_STATE_CODE_TO_REGION[state_code]

    # Fallback al nombre de estado
    return (
        fields.get("STATE_NAME")
        or safe_get(billing_info, "address", "state_name", default="")
        or ""
    )


def extract_direccion(order: dict, billing_info: dict, billing_raw: dict | None = None) -> str:
    # 1. Primero desde additional_info (estructura real MLC)
    direccion = extract_direccion_from_additional_info(billing_info)
    if direccion:
        return direccion

    # 2. Desde address como objeto dict
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

    # 3. Fallback: buscar strings que parezcan dirección
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


def summarize_order_items(order: dict) -> tuple[list[str], int, float]:
    items_summary = []
    item_count = 0
    total_bruto = 0.0

    for item in order.get("order_items", []):
        qty = float(item.get("quantity", 0) or 0)
        title = safe_get(item, "item", "title", default="Producto ML")
        unit_price = float(item.get("unit_price", 0) or 0)
        subtotal = qty * unit_price

        item_count += int(qty)
        total_bruto += subtotal
        items_summary.append(f"{title} x{int(qty)}")

    return items_summary, item_count, round(total_bruto, 2)


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


def get_ml_pack(pack_id: str) -> dict:
    """Retorna todas las órdenes de un pack."""
    return ml_get(f"https://api.mercadolibre.com/packs/{pack_id}")


def get_venta_by_pack(pack_id: str) -> Optional[dict]:
    """Busca una venta existente por pack_id."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM ventas WHERE pack_id = %s LIMIT 1", (str(pack_id),))
            row = cur.fetchone()
    return dict(row) if row else None


def merge_order_items(orders: list) -> list:
    """Consolida los items de múltiples órdenes en una sola lista."""
    all_items = []
    for order in orders:
        all_items.extend(order.get("order_items", []))
    return all_items


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


def get_partner_fields(ctx: OdooCtx) -> set[str]:
    fields = odoo_exec(ctx, "res.partner", "fields_get", [], {"attributes": ["string"]})
    return set(fields.keys())


def get_move_fields(ctx: OdooCtx) -> set[str]:
    fields = odoo_exec(ctx, "account.move", "fields_get", [], {"attributes": ["string"]})
    return set(fields.keys())


def get_journal(ctx: OdooCtx, tipo: str) -> Optional[int]:
    if tipo == "Factura":
        return ODOO_JOURNAL_FACTURA_ID
    return ODOO_JOURNAL_BOLETA_ID


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


def get_activity_field_name(ctx: OdooCtx) -> Optional[str]:
    fields = get_partner_fields(ctx)
    for candidate in (
        "l10n_cl_activity_description",
        "activity_description",
        "x_studio_giro",
        "x_giro",
        "giro",
    ):
        if candidate in fields:
            return candidate
    return None


def rut_con_guion(rut_norm: str) -> str:
    """Convierte RUT normalizado a formato con guion: 123456789 → 12345678-9"""
    if not rut_norm or len(rut_norm) < 2:
        return rut_norm
    return rut_norm[:-1] + "-" + rut_norm[-1]


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    """
    Busca partner por RUT en Odoo.
    Los RUT en Odoo siempre tienen guion (ej: 12345678-9),
    por lo que normalizamos el RUT entrante y buscamos con guion.
    """
    rut_norm = normalize_rut(rut)
    if not rut_norm:
        return None

    rut_odoo = rut_con_guion(rut_norm)
    ids = odoo_exec(
        ctx,
        "res.partner",
        "search",
        [[["vat", "=", rut_odoo]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def find_partner_by_ml_order(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "res.partner",
        "search",
        [[["comment", "ilike", f"ML_ORDER:{order_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_state_id(ctx: OdooCtx, region_name: str) -> Optional[int]:
    """Busca el state_id de Odoo por nombre de región chilena."""
    if not region_name:
        return None
    ids = odoo_exec(ctx, "res.country.state", "search",
        [[["name", "ilike", region_name], ["country_id.code", "=", "CL"]]],
        {"limit": 1})
    return ids[0] if ids else None


def upsert_partner(
    ctx: OdooCtx,
    order_id: str,
    nombre: str,
    rut: str,
    email: str,
    giro: str,
    direccion: str,
    es_empresa: bool,
    tipo: str,
    ciudad: str = "",
    region: str = "",
) -> int:
    partner_fields = get_partner_fields(ctx)
    activity_field = get_activity_field_name(ctx)
    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_id_type(ctx)

    taxpayer_type = "1" if es_empresa else "4"

    rut_odoo = rut_con_guion(normalize_rut(rut)) if rut else ""
    partner_id = find_partner_by_rut(ctx, rut) or find_partner_by_ml_order(ctx, order_id)
    logger.info(f"upsert_partner: rut_odoo='{rut_odoo}' → partner_id={'encontrado:' + str(partner_id) if partner_id else 'no encontrado → se creará nuevo'}")

    giro_a_usar = giro.strip()
    if tipo == "Boleta" and not giro_a_usar:
        giro_a_usar = DEFAULT_BOLETA_ACTIVITY

    vals = {
        "name": nombre,
        "email": email,
        "comment": f"ML_ORDER:{order_id}",
    }

    if rut:
        # Odoo Chile almacena RUT con guion — normalizar al guardar
        vals["vat"] = rut_con_guion(rut)
    if "l10n_cl_dte_email" in partner_fields:
        vals["l10n_cl_dte_email"] = email
    if direccion and "street" in partner_fields:
        vals["street"] = direccion
    # Ciudad/comuna → campo "city" en Odoo
    if ciudad and "city" in partner_fields:
        vals["city"] = ciudad
    # Región → state_id en Odoo
    if region and "state_id" in partner_fields:
        state_id = get_state_id(ctx, region)
        if state_id:
            vals["state_id"] = state_id
    if chile_id and "country_id" in partner_fields:
        vals["country_id"] = chile_id
    if "company_type" in partner_fields:
        vals["company_type"] = "company" if es_empresa else "person"
    if "is_company" in partner_fields:
        vals["is_company"] = es_empresa
    if "l10n_cl_sii_taxpayer_type" in partner_fields:
        vals["l10n_cl_sii_taxpayer_type"] = taxpayer_type
    if rut and rut_type_id and "l10n_latam_identification_type_id" in partner_fields:
        vals["l10n_latam_identification_type_id"] = rut_type_id

    if activity_field:
        if tipo == "Factura":
            if giro_a_usar:
                vals[activity_field] = giro_a_usar
        else:
            vals[activity_field] = giro_a_usar or DEFAULT_BOLETA_ACTIVITY

    if partner_id:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner actualizado: id={partner_id} ciudad={ciudad} region={region}")
        return partner_id

    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado: id={partner_id} empresa={es_empresa} ciudad={ciudad}")
    return partner_id


# =========================
# CREACIÓN DOCUMENTO ODOO
# =========================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "account.move",
        "search",
        [[["ref", "=", str(order_id)], ["state", "in", ["draft", "posted", "cancel"]]]],
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
    ciudad_override: Optional[str] = None,
    region_override: Optional[str] = None,
) -> tuple[int, int]:
    ctx = odoo_connect()
    order_id = str(order["id"])

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return existing, 0

    billing_info = get_billing_info(billing_raw)
    rut = normalize_rut(rut_override) if rut_override else extract_rut(billing_info)
    nombre = (cliente_override or extract_name(billing_info, order) or "Cliente ML").strip()
    direccion = (direccion_override or extract_direccion(order, billing_info, billing_raw) or "").strip()
    ciudad = (ciudad_override or extract_ciudad_from_billing(billing_info) or "").strip()
    region = (region_override or extract_region_from_billing(billing_info) or "").strip()
    email = (email or ML_DEFAULT_EMAIL).strip()
    giro = (giro or "").strip()
    es_empresa = tipo == "Factura"

    if tipo == "Factura" and (not nombre or not rut or not direccion or not giro):
        raise Exception("Para Factura se requiere razón social, RUT, dirección y giro")

    if tipo == "Boleta" and not giro:
        giro = DEFAULT_BOLETA_ACTIVITY

    partner_id = upsert_partner(
        ctx=ctx,
        order_id=order_id,
        nombre=nombre,
        rut=rut,
        email=email,
        giro=giro,
        direccion=direccion,
        es_empresa=es_empresa,
        tipo=tipo,
        ciudad=ciudad,
        region=region,
    )

    journal_id = get_journal(ctx, tipo)
    if not journal_id:
        raise Exception(f"No se encontró diario Odoo para {tipo}")

    doc_type_id = ODOO_DOC_TYPE_FACTURA_ID if tipo == "Factura" else ODOO_DOC_TYPE_BOLETA_ID
    tax_id = get_tax_19(ctx)

    lines = []
    for item in order.get("order_items", []):
        qty = float(item.get("quantity", 0) or 0)
        unit_price_gross = float(item.get("unit_price", 0) or 0)
        price = round(unit_price_gross / IVA_RATE, 2)

        line_vals = {
            "name": safe_get(item, "item", "title", default="Producto ML"),
            "quantity": qty,
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
        "partner_shipping_id": partner_id,
        "ref": str(order_id),
        "invoice_line_ids": lines,
        "l10n_latam_document_type_id": doc_type_id,
        "invoice_payment_term_id": ODOO_PAYMENT_TERM_CONTADO_ID,
        "narration": ODOO_STANDARD_NARRATION,
        "journal_id": journal_id,
    }

    move_fields = get_move_fields(ctx)
    move_vals = {k: v for k, v in move_vals.items() if k in move_fields}

    move_id = odoo_exec(ctx, "account.move", "create", [move_vals])
    odoo_exec(ctx, "account.move", "action_post", [[move_id]])
    logger.info(f"[{order_id}] ✅ Documento creado: move_id={move_id} tipo={tipo}")
    return move_id, partner_id


# =========================
# PROCESAMIENTO WEBHOOK
# =========================

ML_ESTADO_ENVIO = {
    "payment_required": "⏳ Pendiente de pago",
    "payment_in_process": "⏳ Pago en proceso",
    "paid":              "✅ Pagado",
    "ready_to_ship":     "📦 Listo para envío",
    "shipped":           "🚚 En camino",
    "delivered":         "🏠 Entregado",
    "cancelled":         "❌ Cancelado",
    "invalid":           "⚠️ Inválido",
}


def process_webhook_order(order_id: str):
    try:
        order = get_ml_order(order_id)
        if not order:
            logger.warning(f"[{order_id}] Orden no encontrada en ML")
            return

        ml_status = order.get("status", "")
        estado_envio = ML_ESTADO_ENVIO.get(ml_status, ml_status)
        pack_id = str(order.get("pack_id") or "")

        # --- CASO CON PACK: consolidar todas las órdenes del pack ---
        if pack_id:
            # Verificar si el pack ya existe en bandeja
            existing_pack = get_venta_by_pack(pack_id)
            if existing_pack:
                update_venta(existing_pack["id"], estado_envio=estado_envio)
                logger.info(f"[pack:{pack_id}] Estado envío actualizado: {estado_envio}")
                return

            # Solo crear si está pagado
            if ml_status != "paid":
                logger.info(f"[pack:{pack_id}] No pagado ({ml_status}), ignorado")
                return

            # Obtener todas las órdenes del pack
            pack_data = get_ml_pack(pack_id)
            pack_order_ids = [str(o["id"]) for o in (pack_data.get("orders") or [])]

            if not pack_order_ids:
                pack_order_ids = [order_id]

            # Consolidar items de todas las órdenes
            all_orders = []
            for oid_pack in pack_order_ids:
                o = get_ml_order(oid_pack) if oid_pack != order_id else order
                if o:
                    all_orders.append(o)

            all_items = merge_order_items(all_orders)

            # Usar el billing de la orden original
            billing_raw = get_ml_billing_raw(order_id)
            billing_info = get_billing_info(billing_raw)

            rut = extract_rut(billing_info)
            cliente = extract_name(billing_info, order)
            giro = extract_activity(billing_info, order)
            direccion = extract_direccion(order, billing_info, billing_raw)
            ciudad = extract_ciudad_from_billing(billing_info)
            region = extract_region_from_billing(billing_info)
            email = extract_email(billing_info, order)
            tipo_sugerido = detect_tipo(order, billing_info)

            if tipo_sugerido == "Boleta" and not giro:
                giro = DEFAULT_BOLETA_ACTIVITY

            # Guardar con pack_id como ID y todos los items consolidados
            save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro,
                      direccion, email, ciudad, region,
                      pack_id=pack_id, order_items_override=all_items)
            logger.info(f"[pack:{pack_id}] ✅ Pack consolidado: {len(all_orders)} órdenes, {len(all_items)} items")
            return

        # --- CASO SIN PACK: orden simple ---
        existing = get_venta(order_id)
        if existing:
            update_venta(order_id, estado_envio=estado_envio)
            logger.info(f"[{order_id}] Estado envío actualizado: {estado_envio}")
            return

        if ml_status != "paid":
            logger.info(f"[{order_id}] Orden no pagada ({ml_status}), no se agrega a bandeja")
            return

        billing_raw = get_ml_billing_raw(order_id)
        billing_info = get_billing_info(billing_raw)

        rut = extract_rut(billing_info)
        cliente = extract_name(billing_info, order)
        giro = extract_activity(billing_info, order)
        direccion = extract_direccion(order, billing_info, billing_raw)
        ciudad = extract_ciudad_from_billing(billing_info)
        region = extract_region_from_billing(billing_info)
        email = extract_email(billing_info, order)
        tipo_sugerido = detect_tipo(order, billing_info)

        if tipo_sugerido == "Boleta" and not giro:
            giro = DEFAULT_BOLETA_ACTIVITY

        save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro,
                  direccion, email, ciudad, region)
        logger.info(f"[{order_id}] ✅ Guardada: {tipo_sugerido} estado={estado_envio}")
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
    ciudad = extract_ciudad_from_billing(billing_info)
    region = extract_region_from_billing(billing_info)
    email = extract_email(billing_info, order)
    tipo_sugerido = detect_tipo(order, billing_info)

    if tipo_sugerido == "Boleta" and not giro:
        giro = DEFAULT_BOLETA_ACTIVITY

    save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion, email, ciudad, region)

    items, item_count, total_bruto = summarize_order_items(order)

    return {
        "id": str(order["id"]),
        "cliente": cliente,
        "rut": rut,
        "email": email,
        "giro": giro,
        "direccion": direccion,
        "tipo_sugerido": tipo_sugerido,
        "items": items,
        "item_count": item_count,
        "total_bruto": total_bruto,
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
    items = list_ventas(estado)
    # order_json no viene en list_ventas para ahorrar ancho de banda
    # Los productos/totales se muestran como placeholders hasta que se edite/reprocese
    enriched = []
    for v in items:
        enriched.append({
            **v,
            "productos": [],
            "cantidad_items": "—",
            "total_bruto": 0,
        })
    return {"items": enriched}


@app.post("/ventas/reprocesar-todo")
def reprocesar_todo():
    """Reprocesa desde ML todas las ventas pendientes o con error."""
    todas = list_ventas("pendiente") + list_ventas("error")
    resultados = {"ok": 0, "error": 0, "errores": []}
    for venta in todas:
        try:
            reprocesar_venta_desde_ml(str(venta["id"]))
            resultados["ok"] += 1
        except Exception as e:
            resultados["error"] += 1
            resultados["errores"].append({"id": venta["id"], "error": str(e)[:200]})
    return resultados


class AgruparPayload(BaseModel):
    ids: list


@app.post("/ventas/agrupar")
def agrupar_ventas(payload: AgruparPayload):
    """Consolida múltiples ventas en una sola fila."""
    ids = [str(i) for i in payload.ids]
    if len(ids) < 2:
        raise HTTPException(status_code=400, detail="Se necesitan al menos 2 ventas para agrupar")

    ventas_list = []
    for oid in ids:
        v = get_venta(oid)
        if not v:
            raise HTTPException(status_code=404, detail=f"Venta {oid} no encontrada")
        if v.get("estado") == "enviado":
            raise HTTPException(status_code=400, detail=f"La venta {oid} ya fue enviada a Odoo y no se puede agrupar")
        ventas_list.append(v)

    principal = ventas_list[0]
    secundarias = ventas_list[1:]

    try:
        order_principal = json.loads(principal["order_json"])
        all_items = list(order_principal.get("order_items", []))

        for v in secundarias:
            order_sec = json.loads(v["order_json"])
            all_items.extend(order_sec.get("order_items", []))

        order_consolidado = {**order_principal, "order_items": all_items}
        _, item_count, total_bruto = summarize_order_items(order_consolidado)

        update_venta(principal["id"],
            order_json=json.dumps(order_consolidado, ensure_ascii=False))

        for v in secundarias:
            update_venta(v["id"], estado="rechazado",
                error=f"Agrupada en venta {principal['id']}")

        return {
            "ok": True,
            "venta_principal": principal["id"],
            "agrupadas": [v["id"] for v in secundarias],
            "total_items": item_count,
            "total_bruto": total_bruto,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ventas/{oid}")
def venta_detalle(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    order = json.loads(venta.get("order_json") or "{}")
    products, item_count, total_bruto = summarize_order_items(order)
    venta["productos"] = products
    venta["cantidad_items"] = item_count
    venta["total_bruto"] = total_bruto
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

    if updates.get("tipo_sugerido") == "Boleta" and not updates.get("giro") and not venta.get("giro"):
        updates["giro"] = DEFAULT_BOLETA_ACTIVITY

    if updates.get("tipo_sugerido") == "Boleta" and "giro" in updates and not updates["giro"]:
        updates["giro"] = DEFAULT_BOLETA_ACTIVITY

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


@app.get("/ventas/{oid}/pack")
def ver_pack(oid: str):
    """Retorna el detalle de todas las órdenes del pack asociado a esta venta."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    pack_id = venta.get("pack_id")
    if not pack_id:
        # Sin pack_id — mostrar solo la orden propia
        try:
            order = json.loads(venta.get("order_json") or "{}")
            items, item_count, total = summarize_order_items(order)
            return {
                "pack_id": None,
                "ordenes": [{
                    "id": venta["id"],
                    "cliente": venta.get("cliente"),
                    "total": total,
                    "items": items,
                    "item_count": item_count,
                }]
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Con pack_id — consultar ML para obtener todas las órdenes
    try:
        pack_data = get_ml_pack(pack_id)
        order_ids = [str(o["id"]) for o in (pack_data.get("orders") or [])]

        ordenes = []
        total_pack = 0.0
        for order_id in order_ids:
            order = get_ml_order(order_id)
            if not order:
                continue
            items, item_count, total = summarize_order_items(order)
            total_pack += total
            ordenes.append({
                "id": order_id,
                "status": order.get("status"),
                "total": total,
                "items": items,
                "item_count": item_count,
            })

        return {
            "pack_id": pack_id,
            "total_pack": round(total_pack, 2),
            "ordenes": ordenes,
        }
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

        move_id, partner_id = create_document(
            order=order,
            billing_raw=billing,
            tipo=venta.get("tipo_sugerido") or "Boleta",
            email=venta.get("email") or ML_DEFAULT_EMAIL,
            giro=venta.get("giro") or "",
            cliente_override=venta.get("cliente"),
            rut_override=venta.get("rut"),
            direccion_override=venta.get("direccion"),
            ciudad_override=venta.get("ciudad"),
            region_override=venta.get("region"),
        )
        update_venta(
            oid,
            estado="enviado",
            move_id=move_id,
            partner_id=partner_id if partner_id else None,
            error=None,
            enviado_en=datetime.now(),
        )
        return {"ok": True, "id": oid, "move_id": move_id, "partner_id": partner_id}
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
    .wrap { max-width: 1400px; margin: 0 auto; padding: 24px; }
    .topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
    .title { font-size: 26px; font-weight: 700; }
    .subtitle { color: var(--muted); margin-top: 4px; font-size: 14px; }
    .actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    button, select, input, textarea { border-radius: 8px; border: 1px solid var(--border); background: var(--panel); color: var(--text); padding: 9px 12px; font-size: 14px; }
    textarea { width: 100%; min-height: 100px; resize: vertical; }
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
    .pack-btn { background: #7c3aed; color: white; border: none; border-radius: 8px; padding: 9px 12px; font-size: 14px; font-weight: 600; cursor: pointer; }
    .pack-btn:hover { opacity: 0.85; }
    .cb-row { width: 16px; height: 16px; cursor: pointer; accent-color: #7c3aed; }
    tr.seleccionada td { background: rgba(124,58,237,0.08) !important; }
    .small { color: var(--muted); font-size: 12px; margin-top: 3px; }
    .empty { text-align: center; padding: 40px; color: var(--muted); background: var(--panel); border: 1px solid var(--border); border-radius: 14px; }
    .modal { position: fixed; inset: 0; background: rgba(2,6,23,0.8); display: none; align-items: center; justify-content: center; padding: 20px; z-index: 100; }
    .modal.open { display: flex; }
    .modal-card { width: min(900px, 100%); background: var(--panel); border: 1px solid var(--border); border-radius: 16px; padding: 24px; }
    .modal-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 16px; }
    .modal-grid .full { grid-column: 1 / -1; }
    label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 5px; font-weight: 600; }
    .modal-actions { display: flex; gap: 8px; margin-top: 18px; justify-content: flex-end; flex-wrap: wrap; }
    a.link { color: #93c5fd; text-decoration: none; font-size: 13px; }
    ul.compact { margin: 6px 0 0 16px; padding: 0; }
    ul.compact li { margin: 2px 0; }
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
      <option value='pendiente'>Pendiente</option>
      <option value=''>Todos los estados</option>
      <option value='enviado'>Enviado</option>
      <option value='error'>Error</option>
      <option value='rechazado'>Rechazado</option>
    </select>
    <button class='warn' onclick='reprocesarTodo()'>↺ Reprocesar todo</button>
    <button id='btnAgrupar' class='pack-btn' style='display:none' onclick='agruparSeleccionadas()'>⛓ Agrupar seleccionadas</button>
  </div>
  <div id='seleccionInfo' style='display:none;margin-bottom:10px;font-size:13px;color:var(--muted);'>
    <span id='seleccionCount'></span> — selecciona 2 o más ventas del mismo comprador para agruparlas en una sola boleta/factura.
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
        <label>Dirección (calle y número)</label>
        <input id='editDireccion'>
      </div>
      <div>
        <label>Ciudad / Comuna</label>
        <input id='editCiudad'>
      </div>
      <div>
        <label>Región</label>
        <select id='editRegion'>
          <option value=''>-- Seleccionar --</option>
          <option value='Metropolitana'>Metropolitana (RM)</option>
          <option value='Valparaíso'>Valparaíso</option>
          <option value='del BíoBio'>del BíoBio</option>
          <option value='de la Araucania'>de la Araucanía</option>
          <option value='Antofagasta'>Antofagasta</option>
          <option value='Coquimbo'>Coquimbo</option>
          <option value='del Libertador Gral. Bernardo O&apos;Higgins'>O&apos;Higgins</option>
          <option value='del Maule'>del Maule</option>
          <option value='de los Lagos'>de los Lagos</option>
          <option value='Tarapacá'>Tarapacá</option>
          <option value='Atacama'>Atacama</option>
          <option value='Arica y Parinacota'>Arica y Parinacota</option>
          <option value='Aysén del Gral. Carlos Ibáñez del Campo'>Aysén</option>
          <option value='Magallanes'>Magallanes</option>
          <option value='Los Ríos'>Los Ríos</option>
          <option value='del Ñuble'>del Ñuble</option>
        </select>
      </div>
      <div class='full' id='giroGroup' style='display:none'>
        <label>Giro (solo factura)</label>
        <input id='editGiro'>
      </div>
      <div>
        <label>Total bruto ML</label>
        <input id='editTotal' disabled>
      </div>
      <div>
        <label>Cantidad de ítems</label>
        <input id='editItemsCount' disabled>
      </div>
      <div class='full'>
        <label>Productos vendidos</label>
        <textarea id='editProducts' disabled></textarea>
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
  return '<span class="badge ' + (map[estado] || 'badge-default') + '">' + safe(estado) + '</span>';
}

function safe(v) {
  if (v == null || v === '') return '—';
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}
function escAttr(v) {
  if (v == null) return '';
  return String(v)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}


function money(v) {
  const n = Number(v || 0);
  return new Intl.NumberFormat('es-CL', { style: 'currency', currency: 'CLP', maximumFractionDigits: 0 }).format(n);
}

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
    const okQ = !q || [v.id, v.cliente, v.rut, v.email, v.tipo_sugerido, v.direccion, v.giro, ...(v.productos || [])].filter(Boolean).join(' ').toLowerCase().includes(q);
    return okS && okQ;
  });
}

function rowHtml(v) {
  const id = String(v.id || '');
  const fecha = v.creado_en ? new Date(v.creado_en + 'Z').toLocaleString('es-CL', {timeZone: 'America/Santiago'}) : '—';
  const packId = v.pack_id ? String(v.pack_id) : '';

  let acciones = '';
  acciones += '<button class="secondary" data-action="edit" data-id="' + escAttr(id) + '">Editar</button> ';
  acciones += '<button class="warn" data-action="reprocesar" data-id="' + escAttr(id) + '">Reprocesar</button> ';
  if (packId) {
    acciones += '<button class="pack-btn" data-action="verpack" data-id="' + escAttr(id) + '" data-pack="' + escAttr(packId) + '">📦 Ver pack</button> ';
  }
  if (v.estado !== 'enviado') {
    acciones += '<button class="success" data-action="autorizar" data-id="' + escAttr(id) + '">Autorizar</button>';
  }

  let productos = '';
  if ((v.productos || []).length) {
    productos = '<ul class="compact">' + v.productos.slice(0,3).map(function(p){ return '<li>' + safe(p) + '</li>'; }).join('') + '</ul>';
  }

  return '<tr id="row-' + escAttr(id) + '">' +
    '<td><input type="checkbox" class="cb-row" data-id="' + escAttr(id) + '" onchange="onCheckboxChange()"></td>' +
    '<td>' + safe(fecha) + '<div class="small">' + safe(id) + '</div><div class="small"><a href="#" class="link" data-action="copy" data-id="' + escAttr(id) + '">Copiar ID</a></div></td>' +
    '<td><strong>' + safe(v.cliente) + '</strong><div class="small">' + safe(v.email) + '</div>' + (v.giro ? '<div class="small">' + safe(v.giro) + '</div>' : '') + '</td>' +
    '<td>' + safe(v.rut) + '</td>' +
    '<td>' + safe(v.direccion) + (v.ciudad ? '<div class="small">📍 ' + safe(v.ciudad) + '</div>' : '') + (v.region ? '<div class="small">' + safe(v.region) + '</div>' : '') + '</td>' +
    '<td><strong>' + money(v.total_bruto) + '</strong><div class="small">' + safe(v.cantidad_items) + ' ítems</div>' + productos + '</td>' +
    '<td>' + safe(v.tipo_sugerido) + '</td>' +
    '<td>' + badge(v.estado) + (v.error ? '<div class="small" style="color:#f87171;margin-top:4px">' + safe(v.error).substring(0,80) + '</div>' : '') + '</td>' +
    '<td><span style="font-size:13px">' + safe(v.estado_envio || '✅ Pagado') + '</span></td>' +
    '<td><div class="row-actions">' + acciones + '</div></td>' +
    '</tr>';
}

function renderTable() {
  const items = filteredVentas();
  updateStats(ventas);
  const wrap = document.getElementById('tableWrap');
  if (!items.length) {
    wrap.innerHTML = "<div class='empty'>No hay ventas para mostrar.</div>";
    return;
  }

  let html = '<table><thead><tr>';
  html += '<th style="width:32px"><input type="checkbox" class="cb-row" id="cbTodos" onchange="toggleTodos(this)"></th>';
  html += '<th>Fecha / ID ML</th><th>Cliente / Razón social</th><th>RUT</th>';
  html += '<th>Dirección / Ciudad / Región</th><th>Total / Ítems</th>';
  html += '<th>Tipo</th><th>Estado doc.</th><th>Estado envío ML</th><th>Acciones</th>';
  html += '</tr></thead><tbody>';
  items.forEach(function(v) { html += rowHtml(v); });
  html += '</tbody></table>';
  wrap.innerHTML = html;

  // Delegación de eventos — evita problemas con onclick e IDs especiales
  wrap.querySelectorAll('[data-action]').forEach(function(el) {
    el.addEventListener('click', function(e) {
      e.preventDefault();
      const action = el.dataset.action;
      const id = el.dataset.id;
      if (action === 'edit') openEdit(id);
      else if (action === 'reprocesar') reprocesar(id);
      else if (action === 'autorizar') autorizar(id);
      else if (action === 'verpack') verPack(id, el.dataset.pack);
      else if (action === 'copy') { navigator.clipboard.writeText(id); }
    });
  });
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
  if (!confirm('¿Autorizar la venta ' + id + ' y enviarla a Odoo?')) return;
  const res = await fetch('/ventas/' + id + '/autorizar', { method: 'POST' });
  const data = await res.json().catch(() => ({}));
  if (res.ok) alert('✅ Documento creado en Odoo: move_id=' + data.move_id);
  else alert('❌ Error: ' + (data.detail || 'desconocido'));
  await refreshData();
}

function toggleGiro() {
  document.getElementById('giroGroup').style.display =
    document.getElementById('editTipo').value === 'Factura' ? 'block' : 'none';
}

async function openEdit(id) {
  const v = ventas.find(x => String(x.id) === String(id));
  if (!v) return;
  currentId = String(id);
  document.getElementById('modalSub').textContent = 'Venta ' + String(v.id);
  document.getElementById('editId').value = v.id || '';
  document.getElementById('editTipo').value = v.tipo_sugerido || 'Boleta';
  document.getElementById('editEmail').value = v.email || '';
  document.getElementById('editCliente').value = v.cliente || '';
  document.getElementById('editRut').value = v.rut || '';
  document.getElementById('editDireccion').value = v.direccion || '';
  document.getElementById('editCiudad').value = v.ciudad || '';
  document.getElementById('editRegion').value = v.region || '';
  document.getElementById('editGiro').value = v.tipo_sugerido === 'Factura' ? (v.giro || '') : '';
  document.getElementById('editTotal').value = '...';
  document.getElementById('editItemsCount').value = '...';
  document.getElementById('editProducts').value = 'Cargando...';
  toggleGiro();
  document.getElementById('editModal').classList.add('open');

  // Cargar detalle completo (con productos y total) en background
  try {
    const res = await fetch('/ventas/' + id);
    const det = await res.json();
    document.getElementById('editTotal').value = money(det.total_bruto);
    document.getElementById('editItemsCount').value = det.cantidad_items || 0;
    document.getElementById('editProducts').value = (det.productos || []).join(String.fromCharCode(10));
  } catch(e) {
    document.getElementById('editProducts').value = 'Error cargando productos';
  }
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
    ciudad: document.getElementById('editCiudad').value,
    region: document.getElementById('editRegion').value,
    giro: tipo === 'Factura' ? document.getElementById('editGiro').value : '',
  };

  const res = await fetch('/ventas/' + currentId, {
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

// ========================
// CHECKBOX Y AGRUPACIÓN
// ========================

function getSeleccionadas() {
  return Array.from(document.querySelectorAll('.cb-row[data-id]:checked')).map(cb => cb.dataset.id);
}

function onCheckboxChange() {
  const seleccionadas = getSeleccionadas();
  const btn = document.getElementById('btnAgrupar');
  const info = document.getElementById('seleccionInfo');
  const count = document.getElementById('seleccionCount');

  // Resaltar filas seleccionadas
  document.querySelectorAll('.cb-row[data-id]').forEach(cb => {
    const row = document.getElementById('row-' + cb.dataset.id);
    if (row) row.classList.toggle('seleccionada', cb.checked);
  });

  if (seleccionadas.length >= 2) {
    btn.style.display = 'inline-block';
    info.style.display = 'block';
    count.textContent = seleccionadas.length + ' ventas seleccionadas';
  } else {
    btn.style.display = 'none';
    info.style.display = seleccionadas.length === 1 ? 'block' : 'none';
    count.textContent = seleccionadas.length === 1 ? '1 venta seleccionada — selecciona al menos 1 más para agrupar' : '';
  }
}

function toggleTodos(cb) {
  document.querySelectorAll('.cb-row[data-id]').forEach(c => c.checked = cb.checked);
  onCheckboxChange();
}

async function agruparSeleccionadas() {
  const ids = getSeleccionadas();
  if (ids.length < 2) { alert('Selecciona al menos 2 ventas para agrupar'); return; }

  // Mostrar resumen antes de confirmar
  const ventasSelec = ids.map(id => ventas.find(v => String(v.id) === id)).filter(Boolean);
  const resumen = ventasSelec.map(function(v){ return '• ' + safe(v.cliente) + ' — ' + safe(v.id); }).join(String.fromCharCode(10));
  const msg = '¿Agrupar estas ' + ids.length + ' ventas en una sola boleta/factura?' +
    '\n\nLa primera será la principal y las demás quedarán como rechazadas:' +
    '\n\n' + resumen +
    '\n\nEsta acción no se puede deshacer.';
  if (!confirm(msg)) return;

  const res = await fetch('/ventas/agrupar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ids }),
  });
  const data = await res.json().catch(() => ({}));

  if (res.ok) {
    alert('✅ Ventas agrupadas en ' + data.venta_principal + '\n' + data.total_items + ' ítems · Total bruto: ' + money(data.total_bruto));
    document.getElementById('btnAgrupar').style.display = 'none';
    document.getElementById('seleccionInfo').style.display = 'none';
    await refreshData();
  } else {
    alert('❌ Error: ' + (data.detail || 'desconocido'));
  }
}

async function reprocesarTodo() {
  const pendientes = ventas.filter(v => v.estado === 'pendiente' || v.estado === 'error');
  if (!pendientes.length) { alert('No hay ventas pendientes para reprocesar'); return; }
  if (!confirm('¿Reprocesar las ' + pendientes.length + ' ventas pendientes/con error desde ML?\nEsto actualizará los datos de cada venta con la información actual de Mercado Libre.')) return;

  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '↺ Reprocesando...';

  const res = await fetch('/ventas/reprocesar-todo', { method: 'POST' });
  const data = await res.json().catch(() => ({}));

  btn.disabled = false;
  btn.textContent = '↺ Reprocesar todo';

  if (res.ok) {
    alert('✅ Reprocesadas: ' + data.ok + ' | ❌ Errores: ' + data.error);
    await refreshData();
  } else {
    alert('❌ Error: ' + (data.detail || 'desconocido'));
  }
}

async function reprocesar(id) {
  const res = await fetch('/ventas/' + id + '/reprocesar', { method: 'POST' });
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

// ========================
// MODAL PACK
// ========================

async function verPack(id, packId) {
  document.getElementById('packModalTitle').textContent = packId ? ('Pack ' + packId) : ('Orden ' + id);
  document.getElementById('packModalBody').innerHTML = "<p style='color:var(--muted)'>Cargando órdenes del pack...</p>";
  document.getElementById('packModal').classList.add('open');

  try {
    const res = await fetch('/ventas/' + id + '/pack');
    const data = await res.json();
    if (!res.ok) {
      document.getElementById('packModalBody').innerHTML = '<p style="color:#f87171">Error: ' + (data.detail || 'desconocido') + '</p>';
      return;
    }

    const ordenes = data.ordenes || [];
    const totalPack = data.total_pack || 0;

    let html = '';
    if (data.pack_id) {
      html += '<div style="margin-bottom:12px;color:var(--muted);font-size:13px">Pack ID: <strong style="color:var(--text)">' + data.pack_id + '</strong> · ' + ordenes.length + ' orden(es) consolidada(s) · Total: <strong style="color:var(--ok)">' + money(totalPack) + '</strong></div>';
    }

    for (const orden of ordenes) {
          html += '<div style="background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:10px;">' +
        '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">' +
        '<strong style="font-size:13px">Order ID: ' + orden.id + '</strong>' +
        '<span style="font-size:12px;color:var(--muted)">' + (orden.status || '') + ' · ' + orden.item_count + ' ítem(s) · <strong style="color:var(--ok)">' + money(orden.total) + '</strong></span>' +
        '</div><ul style="margin:0;padding-left:18px;">' +
        (orden.items || []).map(function(item){ return '<li style="font-size:13px;color:var(--muted);margin-bottom:3px;">' + item + '</li>'; }).join('') +
        '</ul></div>';
    }

    if (!ordenes.length) {
      html = "<p style='color:var(--muted)'>No se encontraron órdenes en este pack.</p>";
    }

    document.getElementById('packModalBody').innerHTML = html;
  } catch(e) {
    document.getElementById('packModalBody').innerHTML = '<p style="color:#f87171">Error de conexión: ' + e.message + '</p>';
  }
}

function closePackModal() {
  document.getElementById('packModal').classList.remove('open');
}

document.getElementById('packModal').addEventListener('click', e => {
  if (e.target.id === 'packModal') closePackModal();
});

refreshData();
setInterval(() => {
  // No recargar si el modal de edición está abierto
  if (!document.getElementById('editModal').classList.contains('open')) {
    refreshData();
  }
}, 60000);
</script>

<!-- Modal Pack -->
<div class='modal' id='packModal'>
  <div class='modal-card' style='max-width:680px'>
    <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;'>
      <div>
        <div class='title' style='font-size:20px'>📦 Detalle del pack</div>
        <div class='subtitle' id='packModalTitle'></div>
      </div>
      <button class='secondary' onclick='closePackModal()'>Cerrar</button>
    </div>
    <div id='packModalBody'></div>
  </div>
</div>

</body>
</html>
""")
