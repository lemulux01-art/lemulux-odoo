from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
import os
import time
import threading
import queue as queue_module
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
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS tipo_envio_ml TEXT DEFAULT ''",
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

            # Extraer tipo de envio desde shipment
            tipo_envio_ml = extract_logistic_type(order)

            cur.execute(
                """
                INSERT INTO ventas
                    (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region, tipo_sugerido, estado, estado_envio, order_json, billing_json, tipo_envio_ml)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
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
                    tipo_envio_ml,
                ),
            )
        conn.commit()
    logger.info(f"[{oid}] Guardada → tipo={tipo_sugerido} rut={rut_final or 'sin RUT'} cliente={cliente_final}")


def list_ventas(estado: Optional[str] = None) -> list:
    cols = "id, cliente, rut, email, giro, direccion, ciudad, region, tipo_sugerido, estado, estado_envio, pack_id, move_id, partner_id, error, creado_en, enviado_en, order_json, tipo_envio_ml"
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
    # 1. Señal más directa: cust_type de ML
    cust_type = (safe_get(billing_info, "attributes", "cust_type", default="") or "").upper()
    if cust_type == "BU":
        return "Factura"
    if cust_type == "CO":
        return "Boleta"

    # 2. Descripción del tipo de contribuyente
    taxpayer_desc = (
        safe_get(billing_info, "taxes", "taxpayer_type", "description", default="") or ""
    ).strip().lower()
    if taxpayer_desc and any(x in taxpayer_desc for x in [
        "responsable", "empresa", "negocio", "iva", "juridica"
    ]):
        return "Factura"

    # 3. Tiene giro/actividad economica — señal mas confiable de empresa
    if extract_activity(billing_info, order):
        return "Factura"

    # 4. Tiene razon social explicita en campos de empresa
    for key in ("business_name", "company_name", "razon_social", "social_reason", "legal_name"):
        val = billing_info.get(key)
        if isinstance(val, str) and val.strip():
            return "Factura"

    # 5. Nombre contiene palabras clave de empresa (SPA, LTDA, etc)
    nombre = extract_name(billing_info, order) or ""
    nombre_upper = nombre.upper()
    empresa_keywords = [
        " SPA", " LTDA", "S.A.", " SA ", "LIMITADA", "EIRL", " INC",
        " CORP", "COMERCIAL ", "CONSTRUCTORA", "CONSULTORA",
        "INVERSIONES", "HOLDING", "SOCIEDAD ", " CIA", " CIA."
    ]
    if any(kw in nombre_upper for kw in empresa_keywords):
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


_consecutive_429 = 0  # contador global de 429 consecutivos

def ml_get(url: str) -> dict:
    global _consecutive_429
    for attempt in range(6):
        try:
            res = requests.get(url, headers=ml_headers(), timeout=30)
        except requests.RequestException as e:
            logger.error(f"Error de red en {url}: {e}")
            raise

        if res.status_code == 401:
            logger.warning(f"401 en {url}, renovando token...")
            if not refresh_ml_token():
                raise Exception("Token ML invalido y no se pudo renovar")
            continue

        if res.status_code == 429:
            _consecutive_429 += 1
            # Backoff exponencial basado en 429 consecutivos
            wait = min(5 * (attempt + 1) + (_consecutive_429 * 2), 60)
            logger.warning(f"429 en {url}, reintento en {wait}s (consecutivos: {_consecutive_429})")
            time.sleep(wait)
            continue

        if res.status_code == 404:
            _consecutive_429 = 0
            return {}

        res.raise_for_status()
        _consecutive_429 = 0  # reset al tener exito
        return res.json()

    raise Exception(f"ML devolvio demasiados errores para {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_shipment(shipping_id) -> dict:
    """Obtiene datos del envio incluyendo logistic_type."""
    if not shipping_id:
        return {}
    try:
        return ml_get(f"https://api.mercadolibre.com/shipments/{shipping_id}")
    except Exception:
        return {}


def extract_logistic_type(order: dict) -> str:
    """Extrae el tipo de logistica desde la orden o consultando el shipment."""
    LOGISTIC_LABELS = {
        "cross_docking": "Colecta",
        "self_service":  "Colecta",
        "drop_off":      "Colecta",
        "xd_drop_off":   "Flex",
        "default_xd":    "Flex",
        "fulfillment":   "Full",
    }
    # Primero intentar en el order directamente
    shipping = order.get("shipping") or {}
    logistic = shipping.get("logistic_type")
    if logistic:
        return LOGISTIC_LABELS.get(logistic, logistic)
    # Si no hay, consultar /shipments/{id}
    shipping_id = shipping.get("id")
    if shipping_id:
        shipment = get_ml_shipment(shipping_id)
        logistic = shipment.get("logistic_type")
        if logistic:
            return LOGISTIC_LABELS.get(logistic, logistic)
    return ""


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
    "payment_required": "Pendiente de pago",
    "payment_in_process": "Pago en proceso",
    "paid":              "Pagado",
    "ready_to_ship":     "Listo para envio",
    "shipped":           "En camino",
    "delivered":         "Entregado",
    "cancelled":         "Cancelado",
    "invalid":           "Invalido",
}

# Estados que se consideran "pagados" para ingreso al sistema
ML_ESTADOS_VALIDOS = {"paid", "ready_to_ship", "shipped", "delivered"}


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

            # Solo crear si está en estado válido (pagado o posterior)
            if ml_status not in ML_ESTADOS_VALIDOS:
                logger.info(f"[pack:{pack_id}] Estado no valido para facturar ({ml_status}), ignorado")
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

        if ml_status not in ML_ESTADOS_VALIDOS:
            logger.info(f"[{order_id}] Estado no valido para facturar ({ml_status}), ignorado")
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
    venta = get_venta(order_id)

    order = get_ml_order(order_id)
    first_real_order_id = order_id  # ID de orden real para billing

    if not order:
        # Es un pack_id — buscar ordenes reales
        pack_data = get_ml_pack(order_id)
        pack_order_ids = [str(o["id"]) for o in (pack_data.get("orders") or [])]
        if not pack_order_ids:
            raise Exception("Orden/pack no encontrado en ML: " + order_id)
        first_real_order_id = pack_order_ids[0]
        order = get_ml_order(first_real_order_id)
        if not order:
            raise Exception("No se pudo obtener ninguna orden del pack " + order_id)
        all_orders = []
        for oid_pack in pack_order_ids:
            o = get_ml_order(oid_pack) if oid_pack != first_real_order_id else order
            if o:
                all_orders.append(o)
        all_items = merge_order_items(all_orders)
        order = {**order, "order_items": all_items, "id": int(order_id)}
    elif venta and venta.get("pack_id") and str(venta["pack_id"]) != str(order_id):
        pack_id = str(venta["pack_id"])
        pack_data = get_ml_pack(pack_id)
        pack_order_ids = [str(o["id"]) for o in (pack_data.get("orders") or [])]
        first_real_order_id = str(order.get("id", order_id))
        all_orders = [order]
        for oid_pack in pack_order_ids:
            if oid_pack != first_real_order_id:
                o = get_ml_order(oid_pack)
                if o:
                    all_orders.append(o)
        all_items = merge_order_items(all_orders)
        order = {**order, "order_items": all_items}
    else:
        first_real_order_id = str(order.get("id", order_id))

    # Billing siempre con ID de orden real (no pack_id)
    billing_raw = get_ml_billing_raw(first_real_order_id)
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
        "id": str(order_id),
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

# =========================
# COLA DE WEBHOOKS
# =========================

webhook_queue = queue_module.Queue()


def webhook_worker():
    """Worker que procesa webhooks de uno en uno con delay adaptativo."""
    global _consecutive_429
    while True:
        try:
            order_id = webhook_queue.get(timeout=5)
            try:
                process_webhook_order(order_id)
                # Si hubo muchos 429 recientes, esperar más entre órdenes
                base_delay = 3
                extra = min(_consecutive_429 * 2, 30)
                time.sleep(base_delay + extra)
            except Exception as e:
                logger.error(f"[{order_id}] Error en webhook worker: {e}")
                # Si es por 429, esperar más antes de siguiente
                if "demasiados errores" in str(e) or "429" in str(e):
                    time.sleep(30)
                else:
                    time.sleep(3)
            finally:
                webhook_queue.task_done()
        except queue_module.Empty:
            continue


def get_ml_seller_id() -> Optional[str]:
    """Obtiene el seller_id del token actual."""
    try:
        data = ml_get("https://api.mercadolibre.com/users/me")
        return str(data.get("id", ""))
    except Exception as e:
        logger.error(f"No se pudo obtener seller_id: {e}")
        return None



def get_ml_ordenes_recientes(seller_id: str, total: int = 200) -> list:
    """Obtiene las ultimas N ordenes con estados validos para facturar, paginando de 50 en 50."""
    # ML solo permite filtrar por un estado a la vez
    estados = ["paid"]  # ML solo soporta "paid" en orders/search
    todas = []
    por_estado = total // len(estados) + 50  # pedir suficientes de cada estado

    for estado in estados:
        offset = 0
        limit = 50
        while len([o for o in todas if o.get("status") == estado]) < por_estado:
            url = (
                f"https://api.mercadolibre.com/orders/search"
                f"?seller={seller_id}&order.status={estado}&sort=date_desc"
                f"&limit={limit}&offset={offset}"
            )
            try:
                data = ml_get(url)
            except Exception as e:
                logger.warning(f"Error paginando ordenes ML estado={estado} offset={offset}: {e}")
                break
            resultados = data.get("results") or []
            if not resultados:
                break
            todas.extend(resultados)
            offset += limit
            if len(resultados) < limit:
                break
            time.sleep(2)  # respetar rate limit entre paginas

    # Deduplicar por ID y ordenar por fecha desc
    visto = set()
    unicas = []
    for o in todas:
        oid = str(o.get("id", ""))
        if oid and oid not in visto:
            visto.add(oid)
            unicas.append(o)

    unicas.sort(key=lambda o: o.get("date_created", ""), reverse=True)
    return unicas[:total]

def reconciliar_ordenes_ml():
    """
    Consulta las últimas ordenes pagadas en ML y verifica que estén en la BD.
    Si falta alguna, la encola para procesamiento.
    Se ejecuta cada 15 minutos en background.
    """
    while True:
        time.sleep(15 * 60)  # cada 15 minutos
        try:
            seller_id = get_ml_seller_id()
            if not seller_id:
                continue

            # Consultar órdenes pagadas de las últimas 2 horas
            from datetime import timezone
            ahora = datetime.now(timezone.utc)
            desde = ahora.replace(microsecond=0).isoformat().replace("+00:00", ".000Z")
            # ML usa date_last_updated, buscamos las recientes
            ordenes_ml = get_ml_ordenes_recientes(seller_id, total=200)

            if not ordenes_ml:
                continue

            # Obtener IDs que ya están en la BD
            ids_ml = [str(o["id"]) for o in ordenes_ml]
            with get_db() as conn:
                with conn.cursor() as cur:
                    placeholders = ",".join(["%s"] * len(ids_ml))
                    cur.execute(
                        f"SELECT id FROM ventas WHERE id = ANY(%s::text[])",
                        (ids_ml,)
                    )
                    ids_en_bd = {str(row["id"]) for row in cur.fetchall()}

            # Encontrar órdenes que no están en la BD
            faltantes = [oid for oid in ids_ml if oid not in ids_en_bd]

            if faltantes:
                logger.warning(f"Reconciliacion auto: {len(faltantes)} faltantes, encolando hasta 10")
                encoladas_auto = 0
                for oid in faltantes[:10]:
                    if oid not in list(webhook_queue.queue):
                        webhook_queue.put(oid)
                        encoladas_auto += 1
                    time.sleep(0.5)
                logger.info(f"Reconciliacion auto: {encoladas_auto} ordenes encoladas")
            else:
                logger.info(f"Reconciliacion OK: {len(ordenes_ml)} ordenes ML todas en BD")

        except Exception as e:
            logger.error(f"Error en reconciliacion: {e}")


@app.on_event("startup")
async def on_startup():
    wait_for_db()
    t = threading.Thread(target=schedule_token_refresh, daemon=True)
    t.start()
    logger.info("Renovacion automatica de token ML iniciada (cada 5h)")
    w = threading.Thread(target=webhook_worker, daemon=True)
    w.start()
    logger.info("Worker de cola de webhooks iniciado")
    r = threading.Thread(target=reconciliar_ordenes_ml, daemon=True)
    r.start()
    logger.info("Reconciliador de ordenes ML iniciado (cada 15 min)")


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


@app.get("/debug/shipping/{oid}")
def debug_shipping(oid: str):
    """Ver datos de shipping de una orden para diagnosticar tipo de envio."""
    venta = get_venta(oid)
    order_json = {}
    if venta and venta.get("order_json"):
        order_json = json.loads(venta["order_json"])
    shipping = order_json.get("shipping") or {}
    shipping_id = shipping.get("id")
    shipment_data = {}
    if shipping_id:
        try:
            shipment_data = ml_get(f"https://api.mercadolibre.com/shipments/{shipping_id}")
        except Exception as e:
            shipment_data = {"error": str(e)}
    return {
        "order_shipping_field": shipping,
        "shipping_id": shipping_id,
        "shipment_data": shipment_data,
        "logistic_type_in_order": shipping.get("logistic_type"),
        "logistic_type_in_shipment": (shipment_data.get("logistic_type") or
                                       shipment_data.get("shipping_option", {}).get("shipping_method_id")),
    }


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
async def ml_webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body invalido"})

    topic = body.get("topic", "")
    resource = body.get("resource", "")
    if topic != "orders_v2" or not resource:
        return {"ok": True, "ignored": True}

    order_id = resource.strip("/").split("/")[-1]
    if not order_id:
        return {"ok": True}

    # Deduplicar — no encolar si ya está en la cola
    if order_id not in list(webhook_queue.queue):
        webhook_queue.put(order_id)
        logger.info(f"Webhook encolado: {order_id} (cola: {webhook_queue.qsize()})")
    else:
        logger.info(f"Webhook duplicado ignorado: {order_id}")
    return {"ok": True, "order_id": order_id, "queued": webhook_queue.qsize()}


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
    enriched = []
    for v in items:
        try:
            order = json.loads(v.get("order_json") or "{}")
            productos, cantidad_items, total_bruto = summarize_order_items(order)
        except Exception:
            productos, cantidad_items, total_bruto = [], 0, 0.0
        # Usar tipo_envio_ml guardado en BD; si está vacío mostrar "-"
        tipo_envio = v.get("tipo_envio_ml") or "-"
        v.pop("order_json", None)
        v.pop("billing_json", None)
        enriched.append({
            **v,
            "productos": productos,
            "cantidad_items": cantidad_items,
            "total_bruto": total_bruto,
            "tipo_envio": tipo_envio,
        })
    return {"items": enriched}



class VentaManualPayload(BaseModel):
    tipo: str = "Boleta"
    order_id: Optional[str] = None
    cliente: str
    rut: str
    email: str
    direccion: Optional[str] = ""
    ciudad: Optional[str] = ""
    region: Optional[str] = ""
    giro: Optional[str] = ""
    productos: Optional[list] = []  # lista de strings descriptivos
    total_bruto: Optional[float] = 0.0
    autorizar: bool = False


@app.post("/ventas/manual")
def ingresar_venta_manual(payload: VentaManualPayload):
    """Ingresa una venta manualmente sin pasar por ML."""
    try:
        rut_norm = normalize_rut(payload.rut)
        oid = payload.order_id or f"MANUAL-{int(datetime.now().timestamp())}"
        giro_final = payload.giro or (DEFAULT_BOLETA_ACTIVITY if payload.tipo == "Boleta" else "")

        # Construir order_json simulado con los productos
        items = []
        for p in (payload.productos or []):
            items.append({
                "item": {"title": p},
                "quantity": 1,
                "unit_price": 0
            })
        order_fake = {
            "id": oid,
            "status": "paid",
            "order_items": items,
            "buyer": {"email": payload.email}
        }
        billing_fake = {}

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = %s", (str(oid),))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail=f"Ya existe una venta con ID {oid}")
                cur.execute(
                    """INSERT INTO ventas
                        (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region,
                         tipo_sugerido, estado, estado_envio, order_json, billing_json)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', 'paid', %s, %s)
                    """,
                    (
                        str(oid), None,
                        payload.cliente.strip(),
                        rut_norm,
                        payload.email.strip(),
                        giro_final,
                        (payload.direccion or "").strip(),
                        (payload.ciudad or "").strip(),
                        (payload.region or "").strip(),
                        payload.tipo,
                        json.dumps(order_fake, ensure_ascii=False),
                        json.dumps(billing_fake, ensure_ascii=False),
                    )
                )
            conn.commit()

        logger.info(f"Venta manual ingresada: id={oid} cliente={payload.cliente} tipo={payload.tipo}")

        if payload.autorizar:
            move_id, partner_id = create_document(
                order=order_fake,
                billing_raw=billing_fake,
                tipo=payload.tipo,
                email=payload.email,
                giro=giro_final,
                cliente_override=payload.cliente,
                rut_override=rut_norm,
                direccion_override=payload.direccion,
                ciudad_override=payload.ciudad,
                region_override=payload.region,
            )
            update_venta(str(oid), estado="enviado", move_id=move_id,
                        partner_id=partner_id, error=None, enviado_en=datetime.now())
            return {"ok": True, "id": oid, "move_id": move_id, "autorizado": True}

        return {"ok": True, "id": oid, "autorizado": False}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ingresando venta manual: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/ventas/reconciliar")
def reconciliar_manual():
    """Fuerza reconciliacion con ML. Encola todas las faltantes en background con delays."""
    try:
        seller_id = get_ml_seller_id()
        if not seller_id:
            raise HTTPException(status_code=500, detail="No se pudo obtener seller_id de ML")

        ordenes_ml = get_ml_ordenes_recientes(seller_id, total=200)
        ids_ml = [str(o["id"]) for o in ordenes_ml]

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_ml,))
                ids_en_bd = {str(row["id"]) for row in cur.fetchall()}

        faltantes = [oid for oid in ids_ml if oid not in ids_en_bd]

        if not faltantes:
            return {"ok": True, "ordenes_ml": len(ordenes_ml), "en_bd": len(ids_en_bd), "faltantes": 0, "encoladas": []}

        # Lanzar en background para no bloquear el request
        def encolar_en_lotes(lista):
            lote = 10
            for i in range(0, len(lista), lote):
                chunk = lista[i:i+lote]
                encoladas_chunk = 0
                for oid in chunk:
                    if oid not in list(webhook_queue.queue):
                        webhook_queue.put(oid)
                        encoladas_chunk += 1
                logger.info(f"Reconciliacion: encolado lote {i//lote+1} ({encoladas_chunk} ordenes). Cola: {webhook_queue.qsize()}")
                # Esperar a que la cola baje antes del siguiente lote
                wait = 0
                while webhook_queue.qsize() > 5 and wait < 120:
                    time.sleep(5)
                    wait += 5

        t = threading.Thread(target=encolar_en_lotes, args=(faltantes,), daemon=True)
        t.start()

        logger.info(f"Reconciliacion manual: {len(ordenes_ml)} ML, {len(faltantes)} faltantes, procesando en background")
        return {
            "ok": True,
            "ordenes_ml": len(ordenes_ml),
            "en_bd": len(ids_en_bd),
            "faltantes": len(faltantes),
            "mensaje": f"Procesando {len(faltantes)} ventas faltantes en background (lotes de 10). Apareceran progresivamente en el dashboard.",
            "encoladas": faltantes[:5]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/ingresar/{order_id}")
def ingresar_manual(order_id: str):
    """Fuerza la ingesta de una orden ML aunque no haya llegado el webhook."""
    try:
        process_webhook_order(order_id)
        venta = get_venta(order_id)
        if venta:
            return {"ok": True, "id": order_id, "cliente": venta.get("cliente"), "estado": venta.get("estado")}
        return {"ok": True, "id": order_id, "message": "Procesado pero verificar en dashboard"}
    except Exception as e:
        logger.error(f"Error en ingesta manual {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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



class ClientePayload(BaseModel):
    nombre: str
    rut: str
    email: str
    direccion: Optional[str] = ""
    ciudad: Optional[str] = ""
    region: Optional[str] = ""
    giro: Optional[str] = ""
    es_empresa: bool = False


@app.post("/clientes/crear")
def crear_cliente(payload: ClientePayload):
    """Crea un partner directamente en Odoo con los datos proporcionados."""
    try:
        ctx = odoo_connect()
        chile_id = get_chile_country_id(ctx)
        rut_type_id = get_rut_id_type(ctx)
        activity_field = get_activity_field_name(ctx)
        rut_norm = normalize_rut(payload.rut)
        rut_odoo = rut_con_guion(rut_norm) if rut_norm else False
        taxpayer_type = "1" if payload.es_empresa else "4"

        # Verificar si ya existe
        existing = find_partner_by_rut(ctx, payload.rut)
        if existing:
            raise HTTPException(status_code=409, detail=f"Ya existe un cliente con ese RUT (id={existing})")

        vals = {
            "name": payload.nombre.strip(),
            "vat": rut_odoo,
            "email": payload.email.strip(),
            "l10n_cl_dte_email": payload.email.strip(),
            "customer_rank": 1,
            "company_type": "company" if payload.es_empresa else "person",
            "is_company": payload.es_empresa,
            "country_id": chile_id,
            "l10n_cl_sii_taxpayer_type": taxpayer_type,
        }
        if payload.direccion:
            vals["street"] = payload.direccion.strip()
        if payload.ciudad:
            vals["city"] = payload.ciudad.strip()
        if payload.region and "state_id" in get_partner_fields(ctx):
            state_id = get_state_id(ctx, payload.region)
            if state_id:
                vals["state_id"] = state_id
        if rut_norm and rut_type_id:
            vals["l10n_latam_identification_type_id"] = rut_type_id
        if activity_field and payload.giro:
            vals[activity_field] = payload.giro.strip()
        elif payload.es_empresa and payload.giro:
            vals["x_giro"] = payload.giro.strip()

        partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
        logger.info(f"Cliente creado manualmente: id={partner_id} nombre={payload.nombre}")
        return {"ok": True, "partner_id": partner_id, "nombre": payload.nombre}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creando cliente: {e}", exc_info=True)
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
        logger.error(f"[{oid}] Error reprocesando: {e}", exc_info=True)
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


@app.post("/ventas/{oid}/anular")
def anular_venta(oid: str):
    """Anula el documento en Odoo (si existe) y resetea la venta a pendiente para reemitir."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    move_id = venta.get("move_id")
    odoo_result = None
    if move_id:
        try:
            ctx = odoo_connect()
            # Intentar cancelar el asiento en Odoo
            moves = odoo_exec(ctx, "account.move", "search", [[["id", "=", move_id]]])
            if moves:
                state = odoo_exec(ctx, "account.move", "read", [moves], {"fields": ["state"]})[0]["state"]
                if state == "posted":
                    odoo_exec(ctx, "account.move", "button_cancel", [moves])
                    odoo_result = f"Documento {move_id} cancelado en Odoo"
                elif state == "cancel":
                    odoo_result = f"Documento {move_id} ya estaba cancelado"
                else:
                    odoo_result = f"Documento {move_id} en estado {state} - cancelar manualmente en Odoo"
        except Exception as e:
            odoo_result = f"No se pudo cancelar en Odoo: {e}"
            logger.warning(f"[{oid}] Error cancelando en Odoo: {e}")

    # Resetear la venta a pendiente para que se pueda reemitir
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ventas SET estado='pendiente', move_id=NULL, partner_id=NULL, error=NULL, enviado_en=NULL WHERE id=%s",
                (oid,)
            )
        conn.commit()
    logger.info(f"[{oid}] Venta anulada y reseteada a pendiente. Odoo: {odoo_result}")
    return {"ok": True, "id": oid, "odoo": odoo_result, "message": "Venta reseteada a pendiente - puede reemitir"}


# =========================
# DASHBOARD
# =========================

UI_HTML = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lemulux | Bandeja de ventas</title>
  <style>
    :root{--bg:#0f172a;--panel:#111827;--panel2:#1f2937;--border:#334155;--text:#e5e7eb;--muted:#94a3b8;--ok:#22c55e;--warn:#f59e0b;--bad:#ef4444;--blue:#3b82f6;}
    *{box-sizing:border-box;}
    body{margin:0;font-family:Arial,sans-serif;background:var(--bg);color:var(--text);}
    .wrap{max-width:1400px;margin:0 auto;padding:24px;}
    .topbar{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:20px;flex-wrap:wrap;}
    .title{font-size:26px;font-weight:700;}
    .subtitle{color:var(--muted);margin-top:4px;font-size:14px;}
    .actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center;}
    button,select,input,textarea{border-radius:8px;border:1px solid var(--border);background:var(--panel);color:var(--text);padding:9px 12px;font-size:14px;}
    textarea{width:100%;min-height:100px;resize:vertical;}
    button{cursor:pointer;background:var(--blue);border:none;font-weight:600;}
    button.secondary{background:var(--panel2);border:1px solid var(--border);}
    button.success{background:var(--ok);color:#052e16;}
    button.warn{background:var(--warn);color:#3b2300;}
    button.bad{background:var(--bad);color:white;}
    .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px;}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:16px;}
    .card h3{margin:0 0 6px 0;font-size:13px;color:var(--muted);font-weight:600;}
    .card .value{font-size:26px;font-weight:700;}
    .toolbar{display:flex;gap:10px;margin-bottom:8px;flex-wrap:wrap;}
    .toolbar input{flex:1;min-width:220px;}
    table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--border);border-radius:14px;overflow:hidden;}
    th,td{padding:11px 13px;border-bottom:1px solid var(--border);text-align:left;font-size:13px;vertical-align:top;}
    th{background:#0b1220;color:var(--muted);font-weight:700;font-size:12px;text-transform:uppercase;}
    tr:last-child td{border-bottom:none;}
    tr:hover td{background:rgba(255,255,255,0.02);}
    tr.seleccionada td{background:rgba(124,58,237,0.08)!important;}
    .badge{display:inline-block;padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700;}
    .badge-pendiente{background:rgba(245,158,11,0.15);color:#fbbf24;}
    .badge-enviado{background:rgba(34,197,94,0.15);color:#4ade80;}
    .badge-error{background:rgba(239,68,68,0.15);color:#f87171;}
    .badge-default{background:rgba(148,163,184,0.15);color:#cbd5e1;}
    .row-actions{display:flex;gap:6px;flex-wrap:wrap;}
    .pack-btn{background:#7c3aed;color:white;border:none;border-radius:8px;padding:9px 12px;font-size:14px;font-weight:600;cursor:pointer;}
    .cb-row{width:16px;height:16px;cursor:pointer;accent-color:#7c3aed;}
    .small{color:var(--muted);font-size:12px;margin-top:3px;}
    .empty{text-align:center;padding:40px;color:var(--muted);background:var(--panel);border:1px solid var(--border);border-radius:14px;}
    .modal{position:fixed;inset:0;background:rgba(2,6,23,0.8);display:none;align-items:center;justify-content:center;padding:20px;z-index:100;}
    .modal.open{display:flex;}
    .modal-card{width:min(900px,100%);background:var(--panel);border:1px solid var(--border);border-radius:16px;padding:24px;max-height:90vh;overflow-y:auto;}
    .modal-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:16px;}
    .modal-grid .full{grid-column:1/-1;}
    label{display:block;font-size:12px;color:var(--muted);margin-bottom:5px;font-weight:600;}
    .modal-actions{display:flex;gap:8px;margin-top:18px;justify-content:flex-end;flex-wrap:wrap;}
    a.link{color:#93c5fd;text-decoration:none;font-size:13px;}
    ul.compact{margin:6px 0 0 16px;padding:0;}
    ul.compact li{margin:2px 0;}
  </style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div>
      <div class="title">&#x1F6D2; Bandeja de ventas ML &rarr; Odoo</div>
      <div class="subtitle">Revisa, edita, reprocesa y autoriza cada venta antes de crear el documento en Odoo.</div>
    </div>
    <div class="actions">
      <button class="secondary" onclick="refreshData()">&#8635; Actualizar</button>
      <a class="link" href="/health" target="_blank">Health</a>
      <a class="link" href="/ventas" target="_blank">API</a>
    </div>
  </div>
  <div class="grid">
    <div class="card"><h3>Total</h3><div class="value" id="cTotal">&mdash;</div></div>
    <div class="card"><h3>Pendientes</h3><div class="value" id="cPend">&mdash;</div></div>
    <div class="card"><h3>Enviadas</h3><div class="value" id="cEnv">&mdash;</div></div>
    <div class="card"><h3>Con error</h3><div class="value" id="cErr">&mdash;</div></div>
  </div>
  <div class="toolbar">
    <input id="searchInput" placeholder="Buscar por ID, cliente, RUT, email..." oninput="renderTable()">
    <select id="statusFilter" onchange="renderTable()">
      <option value="pendiente">Pendiente</option>
      <option value="">Todos los estados</option>
      <option value="enviado">Enviado</option>
      <option value="error">Error</option>
      <option value="rechazado">Rechazado</option>
    </select>
    <select id="horaCorte" onchange="recalcularTurnos()">
      <option value="14">Corte 14:00</option>
      <option value="15">Corte 15:00</option>
      <option value="13">Corte 13:00</option>
      <option value="12">Corte 12:00</option>
    </select>
    <button class="secondary" onclick="abrirCalendario()" id="btnCalendario">&#128197; Todos los turnos</button>
    <button class="success" onclick="abrirCrearCliente()">+ Crear cliente</button>
    <button class="secondary" onclick="abrirIngresarVenta()" style="background:var(--blue)">+ Ingresar venta</button>
    <button class="warn" onclick="reprocesarTodo()">&#8635; Reprocesar todo</button>
    <button class="secondary" onclick="reconciliarML()" title="Consulta las ultimas 200 ordenes en ML y agrega las que falten en el sistema">&#128279; Reconciliar ML</button>
    <button id="btnAgrupar" class="pack-btn" style="display:none" onclick="agruparSeleccionadas()">&#9935; Agrupar seleccionadas</button>
  </div>
  <div id="selInfo" style="display:none;margin-bottom:10px;font-size:13px;color:var(--muted)"><span id="selCount"></span></div>
  <div id="tableWrap"><div class="empty">Cargando...</div></div>
</div>

<!-- Modal Calendario de turnos -->
<div class="modal" id="calModal">
  <div class="modal-card" style="max-width:520px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div class="title" style="font-size:20px">Seleccionar turno</div>
      <button class="secondary" onclick="cerrarCalendario()">Cerrar</button>
    </div>
    <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;">
      <label style="font-size:13px">Hora de corte:</label>
      <select id="horaCorte2" onchange="sincronizarCorte(this.value); renderCalendario()">
        <option value="14">14:00</option>
        <option value="15">15:00</option>
        <option value="13">13:00</option>
        <option value="12">12:00</option>
      </select>
    </div>
    <div id="calGrid" style="display:grid;grid-template-columns:repeat(7,1fr);gap:6px;text-align:center;"></div>
    <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end;">
      <button class="secondary" onclick="seleccionarTurno(''); cerrarCalendario()">Ver todos</button>
    </div>
  </div>
</div>

<!-- Modal Crear Cliente -->
<div class="modal" id="clienteModal">
  <div class="modal-card" style="max-width:560px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div class="title" style="font-size:20px">Crear cliente en Odoo</div>
      <button class="secondary" onclick="cerrarCrearCliente()">Cerrar</button>
    </div>
    <div class="modal-grid">
      <div class="full">
        <label>Tipo</label>
        <select id="cliTipo" onchange="toggleCliGiro()">
          <option value="persona">Persona natural (Boleta)</option>
          <option value="empresa">Empresa (Factura)</option>
        </select>
      </div>
      <div class="full">
        <label>Nombre / Razon social</label>
        <input id="cliNombre" placeholder="Nombre completo o razon social">
      </div>
      <div>
        <label>RUT</label>
        <input id="cliRut" placeholder="12345678-9">
      </div>
      <div>
        <label>Email DTE</label>
        <input id="cliEmail" placeholder="correo@ejemplo.com">
      </div>
      <div class="full">
        <label>Direccion</label>
        <input id="cliDireccion" placeholder="Calle y numero">
      </div>
      <div>
        <label>Ciudad / Comuna</label>
        <input id="cliCiudad" placeholder="Las Condes">
      </div>
      <div>
        <label>Region</label>
        <select id="cliRegion">
          <option value="">-- Seleccionar --</option>
          <option value="Metropolitana">Metropolitana (RM)</option>
          <option value="Valparaiso">Valparaiso</option>
          <option value="del BioBio">del BioBio</option>
          <option value="de la Araucania">de la Araucania</option>
          <option value="Antofagasta">Antofagasta</option>
          <option value="Coquimbo">Coquimbo</option>
          <option value="del Libertador Gral. Bernardo O'Higgins">O'Higgins</option>
          <option value="del Maule">del Maule</option>
          <option value="de los Lagos">de los Lagos</option>
          <option value="Tarapaca">Tarapaca</option>
          <option value="Atacama">Atacama</option>
          <option value="Arica y Parinacota">Arica y Parinacota</option>
          <option value="Aysen del Gral. Carlos Ibanez del Campo">Aysen</option>
          <option value="Magallanes">Magallanes</option>
          <option value="Los Rios">Los Rios</option>
          <option value="del Nuble">del Nuble</option>
        </select>
      </div>
      <div class="full" id="cliGiroGroup" style="display:none">
        <label>Giro / Actividad economica</label>
        <input id="cliGiro" placeholder="Comercio al por menor">
      </div>
    </div>
    <div id="cliError" style="color:#f87171;font-size:13px;margin-top:12px;display:none"></div>
    <div class="modal-actions">
      <button class="secondary" onclick="cerrarCrearCliente()">Cancelar</button>
      <button class="success" onclick="crearClienteOdoo()">Crear en Odoo</button>
    </div>
  </div>
</div>
<div class="modal" id="editModal">
  <div class="modal-card">
    <div class="topbar" style="margin-bottom:0">
      <div><div class="title" style="font-size:20px">&#9999;&#65039; Editar venta</div><div class="subtitle" id="modalSub"></div></div>
      <button class="secondary" onclick="closeModal()">Cerrar</button>
    </div>
    <div class="modal-grid">
      <div><label>ID venta ML</label><input id="editId" disabled></div>
      <div><label>Tipo documento</label><select id="editTipo" onchange="toggleGiro()"><option value="Boleta">Boleta</option><option value="Factura">Factura</option></select></div>
      <div><label>Email DTE</label><input id="editEmail"></div>
      <div><label>RUT</label><input id="editRut"></div>
      <div class="full"><label>Nombre cliente</label><input id="editCliente"></div>
      <div class="full"><label>Direccion</label><input id="editDireccion"></div>
      <div><label>Ciudad / Comuna</label><input id="editCiudad"></div>
      <div><label>Region</label><select id="editRegion">
        <option value="">-- Seleccionar --</option>
        <option value="Metropolitana">Metropolitana (RM)</option>
        <option value="Valparaiso">Valparaiso</option>
        <option value="del BioBio">del BioBio</option>
        <option value="de la Araucania">de la Araucania</option>
        <option value="Antofagasta">Antofagasta</option>
        <option value="Coquimbo">Coquimbo</option>
        <option value="del Libertador Gral. Bernardo O'Higgins">O'Higgins</option>
        <option value="del Maule">del Maule</option>
        <option value="de los Lagos">de los Lagos</option>
        <option value="Tarapaca">Tarapaca</option>
        <option value="Atacama">Atacama</option>
        <option value="Arica y Parinacota">Arica y Parinacota</option>
        <option value="Aysen del Gral. Carlos Ibanez del Campo">Aysen</option>
        <option value="Magallanes">Magallanes</option>
        <option value="Los Rios">Los Rios</option>
        <option value="del Nuble">del Nuble</option>
      </select></div>
      <div class="full" id="giroGroup" style="display:none"><label>Giro (solo factura)</label><input id="editGiro"></div>
      <div><label>Total bruto ML</label><input id="editTotal" disabled></div>
      <div><label>Cantidad items</label><input id="editItemsCount" disabled></div>
      <div class="full"><label>Productos vendidos</label><textarea id="editProducts" disabled></textarea></div>
    </div>
    <div class="modal-actions">
      <button class="secondary" onclick="closeModal()">Cancelar</button>
      <button class="warn" onclick="reprocesarActual()">Reprocesar desde ML</button>
      <button onclick="saveEdit()">Guardar</button>
      <button class="success" onclick="saveAndAuthorize()">Guardar y autorizar</button>
    </div>
  </div>
</div>
<div class="modal" id="packModal">
  <div class="modal-card" style="max-width:680px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div><div class="title" style="font-size:20px">Pack</div><div class="subtitle" id="packModalTitle"></div></div>
      <button class="secondary" onclick="closePackModal()">Cerrar</button>
    </div>
    <div id="packModalBody"></div>
  </div>
</div>
<!-- Modal Ingresar Venta Manual -->
<div class="modal" id="ventaManualModal">
  <div class="modal-card" style="max-width:700px">
    <div class="topbar" style="margin-bottom:0">
      <div><div class="title" style="font-size:20px">+ Ingresar venta manual</div>
      <div class="subtitle">Crea una venta directamente sin pasar por ML</div></div>
      <button class="secondary" onclick="cerrarIngresarVenta()">Cerrar</button>
    </div>
    <div class="modal-grid" style="margin-top:16px">
      <div>
        <label>Tipo documento</label>
        <select id="vmTipo" onchange="toggleVmGiro()">
          <option value="Boleta">Boleta</option>
          <option value="Factura">Factura</option>
        </select>
      </div>
      <div>
        <label>ID orden ML (opcional)</label>
        <input id="vmOrderId" placeholder="ej: 2000012419074761">
      </div>
      <div class="full">
        <label>Nombre / Razon social</label>
        <input id="vmCliente" placeholder="Nombre completo o razon social">
      </div>
      <div>
        <label>RUT</label>
        <input id="vmRut" placeholder="12345678-9">
      </div>
      <div>
        <label>Email DTE</label>
        <input id="vmEmail" placeholder="correo@ejemplo.com" value="boleta@lemulux.com">
      </div>
      <div class="full">
        <label>Direccion</label>
        <input id="vmDireccion" placeholder="Calle y numero">
      </div>
      <div>
        <label>Ciudad / Comuna</label>
        <input id="vmCiudad" placeholder="Las Condes">
      </div>
      <div>
        <label>Region</label>
        <select id="vmRegion">
          <option value="">-- Seleccionar --</option>
          <option value="Metropolitana">Metropolitana (RM)</option>
          <option value="Valparaiso">Valparaiso</option>
          <option value="del BioBio">del BioBio</option>
          <option value="de la Araucania">de la Araucania</option>
          <option value="Antofagasta">Antofagasta</option>
          <option value="Coquimbo">Coquimbo</option>
          <option value="del Libertador Gral. Bernardo O'Higgins">O'Higgins</option>
          <option value="del Maule">del Maule</option>
          <option value="de los Lagos">de los Lagos</option>
          <option value="Tarapaca">Tarapaca</option>
          <option value="Atacama">Atacama</option>
          <option value="Arica y Parinacota">Arica y Parinacota</option>
          <option value="Aysen del Gral. Carlos Ibanez del Campo">Aysen</option>
          <option value="Magallanes">Magallanes</option>
          <option value="Los Rios">Los Rios</option>
          <option value="del Nuble">del Nuble</option>
        </select>
      </div>
      <div class="full" id="vmGiroGroup" style="display:none">
        <label>Giro / Actividad economica</label>
        <input id="vmGiro" placeholder="Comercio al por menor">
      </div>
      <div class="full">
        <label>Productos (uno por linea, formato: Descripcion x cantidad $precio_unitario)</label>
        <textarea id="vmProductos" style="min-height:120px" placeholder="Foco Led 24w x2 $9990&#10;Tubo Led T8 x1 $4990"></textarea>
      </div>
    </div>
    <div id="vmError" style="color:#f87171;font-size:13px;margin-top:12px;display:none"></div>
    <div class="modal-actions">
      <button class="secondary" onclick="cerrarIngresarVenta()">Cancelar</button>
      <button onclick="guardarVentaManual()">Guardar como pendiente</button>
      <button class="success" onclick="guardarYAutorizarManual()">Guardar y autorizar</button>
    </div>
  </div>
</div>
<script src="/ui/app.js"></script>
</body>
</html>
"""


UI_JS = '''
var ventas = [];
var currentId = null;
var turnoActivo = '';

function badge(estado) {
  var map = {pendiente:'badge-pendiente', enviado:'badge-enviado', error:'badge-error', rechazado:'badge-default'};
  return '<span class="badge ' + (map[estado] || 'badge-default') + '">' + esc(estado) + '</span>';
}

function enviobadge(tipo) {
  if (!tipo || tipo === '-' || tipo === 'No especificado') {
    return '<span style="font-size:12px;color:#94a3b8">-</span>';
  }
  var colors = {
    'Colecta': 'background:#1e3a5f;color:#93c5fd',
    'Flex':    'background:#14532d;color:#86efac',
    'Full':    'background:#4c1d95;color:#c4b5fd'
  };
  var style = colors[tipo] || 'background:#1f2937;color:#94a3b8';
  return '<span style="' + style + ';padding:3px 8px;border-radius:999px;font-size:11px;font-weight:700">' + esc(tipo) + '</span>';
}

function safe(v) {
  if (v == null || v === '') return '-';
  return String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function esc(v) {
  if (v == null) return '';
  return String(v).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/'/g,'&#39;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function money(v) {
  var n = Number(v || 0);
  return new Intl.NumberFormat('es-CL', {style:'currency', currency:'CLP', maximumFractionDigits:0}).format(n);
}

function getHoraCorte() {
  return parseInt(document.getElementById('horaCorte').value || '14', 10);
}

function getTurnoKey(fechaStr) {
  if (!fechaStr) return '';
  var d = new Date(fechaStr + 'Z');
  var chileOffset = -3 * 60;
  var localMs = d.getTime() + chileOffset * 60000;
  var local = new Date(localMs);
  var hora = local.getUTCHours();
  var corte = getHoraCorte();
  var turnoDate = new Date(localMs);
  if (hora < corte) {
    turnoDate = new Date(localMs - 24 * 60 * 60 * 1000);
  }
  var y = turnoDate.getUTCFullYear();
  var m = String(turnoDate.getUTCMonth() + 1).padStart(2, '0');
  var dd = String(turnoDate.getUTCDate()).padStart(2, '0');
  return y + '-' + m + '-' + dd;
}

function getTurnoLabel(key) {
  if (!key) return '';
  var corte = getHoraCorte();
  var partes = key.split('-');
  var d1 = new Date(Date.UTC(parseInt(partes[0]), parseInt(partes[1])-1, parseInt(partes[2])));
  var d2 = new Date(d1.getTime() + 24 * 60 * 60 * 1000);
  function fmt(d) {
    return String(d.getUTCDate()).padStart(2,'0') + '/' + String(d.getUTCMonth()+1).padStart(2,'0');
  }
  return fmt(d1) + ' ' + String(corte).padStart(2,'0') + ':00 - ' + fmt(d2) + ' ' + String(corte).padStart(2,'0') + ':00';
}

function seleccionarTurno(key) {
  turnoActivo = key;
  var btn = document.getElementById('btnCalendario');
  if (!key) {
    btn.textContent = '[Cal] Todos los turnos';
    btn.style.background = 'var(--panel2)';
  } else {
    btn.textContent = '[Cal] ' + getTurnoLabel(key);
    btn.style.background = 'var(--blue)';
  }
  cerrarCalendario();
  renderTable();
}

function updateStats(items) {
  document.getElementById('cTotal').textContent = ventas.length;
  document.getElementById('cPend').textContent = ventas.filter(function(v){ return v.estado === 'pendiente'; }).length;
  document.getElementById('cEnv').textContent = ventas.filter(function(v){ return v.estado === 'enviado'; }).length;
  document.getElementById('cErr').textContent = ventas.filter(function(v){ return v.estado === 'error'; }).length;
}

function filteredVentas() {
  var q = document.getElementById('searchInput').value.trim().toLowerCase();
  var s = document.getElementById('statusFilter').value;
  return ventas.filter(function(v) {
    var okS = !s || v.estado === s;
    var campos = [v.id, v.cliente, v.rut, v.email, v.tipo_sugerido, v.direccion, v.giro].filter(Boolean).join(' ').toLowerCase();
    var okQ = !q || campos.indexOf(q) >= 0;
    var okT = !turnoActivo || getTurnoKey(v.creado_en) === turnoActivo;
    return okS && okQ && okT;
  });
}

function rowHtml(v) {
  var id = String(v.id || '');
  var fecha = v.creado_en ? new Date(v.creado_en + 'Z').toLocaleString('es-CL', {timeZone:'America/Santiago'}) : '-';

  var acciones = '';
  acciones += '<button class="secondary" data-action="edit" data-id="' + esc(id) + '">Editar</button> ';
  acciones += '<button class="warn" data-action="reprocesar" data-id="' + esc(id) + '">Reprocesar</button> ';
  if (v.pack_id) {
    acciones += '<button class="pack-btn" data-action="verpack" data-id="' + esc(id) + '" data-pack="' + esc(v.pack_id) + '">Pack</button> ';
  }
  if (v.estado !== 'enviado') {
    acciones += '<button class="success" data-action="autorizar" data-id="' + esc(id) + '">Autorizar</button>';
  }
  if (v.estado === 'enviado') {
    acciones += '<button class="bad" data-action="anular" data-id="' + esc(id) + '">Anular</button>';
  }

  return '<tr id="row-' + esc(id) + '">' +
    '<td><input type="checkbox" class="cb-row" data-id="' + esc(id) + '" onchange="onCheckboxChange()"></td>' +
    '<td>' + safe(fecha) + '<div class="small">' + safe(id) + '</div>' +
      '<div class="small"><a href="#" class="link" data-action="copy" data-id="' + esc(id) + '">Copiar ID</a></div></td>' +
    '<td><strong>' + safe(v.cliente) + '</strong>' +
      '<div class="small">' + safe(v.email) + '</div>' +
      (v.giro && v.giro !== '(boleta)' ? '<div class="small">' + safe(v.giro) + '</div>' : '') + '</td>' +
    '<td>' + safe(v.rut) + '</td>' +
    '<td>' + safe(v.direccion) +
      (v.ciudad ? '<div class="small">' + safe(v.ciudad) + '</div>' : '') +
      (v.region ? '<div class="small">' + safe(v.region) + '</div>' : '') + '</td>' +
    '<td><strong>' + money(v.total_bruto) + '</strong>' +
      '<div class="small">' + safe(v.cantidad_items) + ' items</div>' +
      (v.productos && v.productos.length ? '<ul class="compact">' + v.productos.slice(0,3).map(function(p){ return '<li>' + safe(p) + '</li>'; }).join('') + '</ul>' : '') + '</td>' +
    '<td>' + safe(v.tipo_sugerido) + '</td>' +
    '<td>' + enviobadge(v.tipo_envio) + '</td>' +
    '<td>' + badge(v.estado) +
      (v.error ? '<div class="small" style="color:#f87171;margin-top:4px">' + safe(v.error).substring(0,80) + '</div>' : '') + '</td>' +
    '<td><span>' + safe(v.estado_envio || 'paid') + '</span></td>' +
    '<td><div class="row-actions">' + acciones + '</div></td>' +
    '</tr>';
}

function renderTable() {
  var items = filteredVentas();
  updateStats(items);
  var wrap = document.getElementById('tableWrap');
  if (!items.length) {
    wrap.innerHTML = '<div class="empty">No hay ventas para mostrar.</div>';
    return;
  }

  var html = '<table><thead><tr>';
  html += '<th style="width:32px"><input type="checkbox" class="cb-row" id="cbTodos" onchange="toggleTodos(this)"></th>';
  html += '<th>Fecha / ID ML</th><th>Cliente</th><th>RUT</th>';
  html += '<th>Direccion / Ciudad / Region</th><th>Total / Items</th>';
  html += '<th>Tipo</th><th>Envio</th><th>Estado doc.</th><th>Estado envio ML</th><th>Acciones</th>';
  html += '</tr></thead><tbody>';
  for (var i = 0; i < items.length; i++) { html += rowHtml(items[i]); }
  html += '</tbody></table>';
  wrap.innerHTML = html;

  wrap.querySelectorAll('[data-action]').forEach(function(el) {
    el.addEventListener('click', function(e) {
      e.preventDefault();
      var action = el.dataset.action;
      var id = el.dataset.id;
      if (action === 'edit') openEdit(id);
      else if (action === 'reprocesar') reprocesar(id);
      else if (action === 'autorizar') autorizar(id);
      else if (action === 'anular') anular(id);
      else if (action === 'verpack') verPack(id, el.dataset.pack);
      else if (action === 'copy') { try { navigator.clipboard.writeText(id); } catch(e2) {} }
    });
  });
}

function refreshData() {
  document.getElementById('tableWrap').innerHTML = '<div class="empty">Cargando...</div>';
  fetch('/ventas')
    .then(function(r){ return r.json(); })
    .then(function(data) {
      ventas = Array.isArray(data.items) ? data.items : [];
      renderTable();
    })
    .catch(function() {
      document.getElementById('tableWrap').innerHTML = '<div class="empty">Error cargando datos.</div>';
    });
}

function refreshSilente() {
  fetch('/ventas')
    .then(function(r){ return r.json(); })
    .then(function(data) {
      var nuevas = Array.isArray(data.items) ? data.items : [];
      var cambio = nuevas.length !== ventas.length;
      if (!cambio) {
        for (var i = 0; i < nuevas.length; i++) {
          var nv = nuevas[i];
          var ov = null;
          for (var j = 0; j < ventas.length; j++) {
            if (String(ventas[j].id) === String(nv.id)) { ov = ventas[j]; break; }
          }
          if (!ov || ov.estado !== nv.estado || ov.estado_envio !== nv.estado_envio || ov.move_id !== nv.move_id) {
            cambio = true; break;
          }
        }
      }
      if (cambio) {
        ventas = nuevas;
        renderTable();
      }
    })
    .catch(function() {});
}

function autorizar(id) {
  if (!confirm('Autorizar la venta ' + id + ' y enviarla a Odoo?')) return;
  fetch('/ventas/' + id + '/autorizar', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) alert('Documento creado en Odoo: move_id=' + data.move_id);
      else alert('Error: ' + (data.detail || 'desconocido'));
      refreshData();
    });
}

function anular(id) {
  if (!confirm('Anular el documento Odoo de la venta ' + id + ' y resetear a pendiente para reemitir?')) return;
  fetch('/ventas/' + id + '/anular', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        alert('Venta anulada. ' + (data.odoo || '') + ' Ahora puede editar y reautorizar.');
        refreshData();
      } else {
        alert('Error: ' + (data.detail || 'desconocido'));
      }
    });
}

function toggleGiro() {
  document.getElementById('giroGroup').style.display =
    document.getElementById('editTipo').value === 'Factura' ? 'block' : 'none';
}

function openEdit(id) {
  var v = null;
  for (var i = 0; i < ventas.length; i++) {
    if (String(ventas[i].id) === String(id)) { v = ventas[i]; break; }
  }
  if (!v) return;
  currentId = String(id);
  document.getElementById('modalSub').textContent = 'Venta ' + v.id;
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
  fetch('/ventas/' + id)
    .then(function(r){ return r.json(); })
    .then(function(det) {
      document.getElementById('editTotal').value = money(det.total_bruto);
      document.getElementById('editItemsCount').value = det.cantidad_items || 0;
      document.getElementById('editProducts').value = (det.productos || []).join(String.fromCharCode(10));
    })
    .catch(function() {
      document.getElementById('editProducts').value = 'Error cargando productos';
    });
}

function closeModal() {
  currentId = null;
  document.getElementById('editModal').classList.remove('open');
}

function saveEdit() {
  if (!currentId) return Promise.resolve(false);
  var tipo = document.getElementById('editTipo').value;
  var payload = {
    tipo_sugerido: tipo,
    email: document.getElementById('editEmail').value,
    cliente: document.getElementById('editCliente').value,
    rut: document.getElementById('editRut').value,
    direccion: document.getElementById('editDireccion').value,
    ciudad: document.getElementById('editCiudad').value,
    region: document.getElementById('editRegion').value,
    giro: tipo === 'Factura' ? document.getElementById('editGiro').value : ''
  };
  return fetch('/ventas/' + currentId, {
    method: 'PATCH',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r) {
    if (!r.ok) { alert('No se pudo guardar'); return false; }
    closeModal();
    refreshData();
    return true;
  });
}

function saveAndAuthorize() {
  if (!currentId) return;
  var id = currentId;
  saveEdit().then(function(ok) { if (ok) autorizar(id); });
}

function reprocesar(id) {
  fetch('/ventas/' + id + '/reprocesar', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (!data.ok) { alert(data.detail || 'No se pudo reprocesar'); return; }
      alert('Venta reprocesada desde Mercado Libre');
      refreshData();
    });
}

function reprocesarActual() {
  if (!currentId) return;
  reprocesar(currentId);
}

function onCheckboxChange() {
  var seleccionadas = getSeleccionadas();
  var btn = document.getElementById('btnAgrupar');
  var info = document.getElementById('selInfo');
  var count = document.getElementById('selCount');
  document.querySelectorAll('.cb-row[data-id]').forEach(function(cb) {
    var row = document.getElementById('row-' + cb.dataset.id);
    if (row) row.classList.toggle('seleccionada', cb.checked);
  });
  if (seleccionadas.length >= 2) {
    btn.style.display = 'inline-block';
    info.style.display = 'block';
    count.textContent = seleccionadas.length + ' ventas seleccionadas';
  } else {
    btn.style.display = 'none';
    info.style.display = seleccionadas.length === 1 ? 'block' : 'none';
    count.textContent = seleccionadas.length === 1 ? '1 venta seleccionada - selecciona al menos 1 mas para agrupar' : '';
  }
}

function toggleTodos(cb) {
  document.querySelectorAll('.cb-row[data-id]').forEach(function(c) { c.checked = cb.checked; });
  onCheckboxChange();
}

function getSeleccionadas() {
  return Array.from(document.querySelectorAll('.cb-row[data-id]:checked')).map(function(cb){ return cb.dataset.id; });
}

function agruparSeleccionadas() {
  var ids = getSeleccionadas();
  if (ids.length < 2) { alert('Selecciona al menos 2 ventas para agrupar'); return; }
  var resumen = ids.map(function(id) {
    var v = null;
    for (var i = 0; i < ventas.length; i++) { if (String(ventas[i].id) === id) { v = ventas[i]; break; } }
    return v ? ('* ' + (v.cliente || id) + ' - ' + id) : id;
  }).join(String.fromCharCode(10));
  if (!confirm('Agrupar ' + ids.length + ' ventas en una sola boleta/factura? La primera sera la principal: ' + resumen + ' Esta accion no se puede deshacer.')) return;
  fetch('/ventas/agrupar', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ids: ids})
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        alert('Ventas agrupadas en ' + data.venta_principal + '. Items: ' + data.total_items + '. Total: ' + money(data.total_bruto));
        document.getElementById('btnAgrupar').style.display = 'none';
        document.getElementById('selInfo').style.display = 'none';
        refreshData();
      } else {
        alert('Error: ' + (data.detail || 'desconocido'));
      }
    });
}

function reconciliarML() {
  var btn = document.querySelector('[onclick="reconciliarML()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Consultando ML...'; }
  fetch('/ventas/reconciliar', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar ML'; }
      if (data.ok) {
        var msg = 'ML: ' + data.ordenes_ml + ' ordenes | BD: ' + data.en_bd + ' | Faltantes: ' + data.faltantes;
        if (data.mensaje) {
          msg += ' ' + data.mensaje;
          setTimeout(refreshData, 10000);
          setTimeout(refreshData, 30000);
          setTimeout(refreshData, 60000);
        }
        if (data.encoladas && data.encoladas.length > 0) {
          msg += ' - Encoladas: ' + data.encoladas.join(', ');
          setTimeout(refreshData, 5000);
        }
        alert(msg);
      } else {
        alert('Error: ' + (data.detail || 'desconocido'));
      }
    })
    .catch(function(e) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar ML'; }
      alert('Error de conexion: ' + e.message);
    });
}

function reprocesarTodo() {
  var pendientes = ventas.filter(function(v){ return v.estado === 'pendiente' || v.estado === 'error'; });
  if (!pendientes.length) { alert('No hay ventas pendientes para reprocesar'); return; }
  if (!confirm('Reprocesar las ' + pendientes.length + ' ventas pendientes/con error desde ML?')) return;
  var btn = document.querySelector('[onclick="reprocesarTodo()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Reprocesando...'; }
  fetch('/ventas/reprocesar-todo', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reprocesar todo'; }
      alert('Reprocesadas: ' + data.ok + ' | Errores: ' + data.error);
      refreshData();
    });
}

function verPack(id, packId) {
  document.getElementById('packModalTitle').textContent = packId ? ('Pack ' + packId) : ('Orden ' + id);
  document.getElementById('packModalBody').innerHTML = '<p style="color:#94a3b8">Cargando ordenes del pack...</p>';
  document.getElementById('packModal').classList.add('open');
  fetch('/ventas/' + id + '/pack')
    .then(function(r){ return r.json(); })
    .then(function(data) {
      var ordenes = data.ordenes || [];
      var html = '';
      if (data.pack_id) {
        html += '<div style="margin-bottom:12px;font-size:13px">Pack ID: <strong>' + safe(data.pack_id) + '</strong> - ' + ordenes.length + ' orden(es) - Total: <strong>' + money(data.total_pack) + '</strong></div>';
      }
      for (var i = 0; i < ordenes.length; i++) {
        var o = ordenes[i];
        html += '<div style="background:#1f2937;border:1px solid #334155;border-radius:10px;padding:14px;margin-bottom:10px;">';
        html += '<div style="display:flex;justify-content:space-between;margin-bottom:8px;">';
        html += '<strong>Order ID: ' + safe(o.id) + '</strong>';
        html += '<span style="color:#94a3b8">' + safe(o.status) + ' - ' + o.item_count + ' item(s) - <strong>' + money(o.total) + '</strong></span>';
        html += '</div><ul style="margin:0;padding-left:18px;">';
        (o.items || []).forEach(function(item) {
          html += '<li style="font-size:13px;color:#94a3b8;margin-bottom:3px;">' + safe(item) + '</li>';
        });
        html += '</ul></div>';
      }
      if (!ordenes.length) html = '<p style="color:#94a3b8">No se encontraron ordenes.</p>';
      document.getElementById('packModalBody').innerHTML = html;
    })
    .catch(function(e) {
      document.getElementById('packModalBody').innerHTML = '<p style="color:#f87171">Error: ' + e.message + '</p>';
    });
}

function closePackModal() {
  document.getElementById('packModal').classList.remove('open');
}

// ========================
// CALENDARIO DE TURNOS
// ========================

function abrirCalendario() {
  document.getElementById('horaCorte2').value = document.getElementById('horaCorte').value;
  renderCalendario();
  document.getElementById('calModal').classList.add('open');
}

function cerrarCalendario() {
  document.getElementById('calModal').classList.remove('open');
}

function sincronizarCorte(val) {
  document.getElementById('horaCorte').value = val;
  renderTable();
}

function renderCalendario() {
  var grid = document.getElementById('calGrid');
  var conteo = {};
  ventas.forEach(function(v) {
    var k = getTurnoKey(v.creado_en);
    if (k) conteo[k] = (conteo[k] || 0) + 1;
  });
  var keys = Object.keys(conteo).sort().reverse();
  if (!keys.length) {
    grid.innerHTML = '<div style="grid-column:1/-1;color:#94a3b8;padding:20px">No hay turnos disponibles</div>';
    return;
  }
  var dias = ['Dom','Lun','Mar','Mie','Jue','Vie','Sab'];
  var html = dias.map(function(d) {
    return '<div style="font-size:11px;color:#94a3b8;padding:4px 0;font-weight:700">' + d + '</div>';
  }).join('');
  var first = keys[keys.length-1];
  var last = keys[0];
  var p = first.split('-');
  var startDate = new Date(Date.UTC(parseInt(p[0]), parseInt(p[1])-1, parseInt(p[2])));
  var dow = startDate.getUTCDay();
  startDate = new Date(startDate.getTime() - dow * 86400000);
  var p2 = last.split('-');
  var endDate = new Date(Date.UTC(parseInt(p2[0]), parseInt(p2[1])-1, parseInt(p2[2])));
  var cur = new Date(startDate.getTime());
  while (cur <= endDate) {
    var y = cur.getUTCFullYear();
    var m = String(cur.getUTCMonth()+1).padStart(2,'0');
    var d = String(cur.getUTCDate()).padStart(2,'0');
    var key = y + '-' + m + '-' + d;
    var cnt = conteo[key] || 0;
    var isActive = turnoActivo === key;
    var bg = isActive ? 'var(--blue)' : (cnt > 0 ? 'var(--panel2)' : 'transparent');
    var border = cnt > 0 ? '1px solid var(--border)' : '1px solid transparent';
    var cursor = cnt > 0 ? 'pointer' : 'default';
    var clickAttr = cnt > 0 ? ' data-turno="' + key + '"' : '';
    html += '<div class="cal-day" style="border-radius:8px;padding:6px 2px;background:' + bg + ';border:' + border + ';cursor:' + cursor + '"' + clickAttr + '>';
    html += '<div style="font-size:13px;font-weight:600">' + parseInt(d) + '</div>';
    if (cnt > 0) {
      html += '<div style="font-size:11px;color:' + (isActive ? 'white' : '#22c55e') + '">' + cnt + '</div>';
    }
    html += '</div>';
    cur = new Date(cur.getTime() + 86400000);
  }
  grid.innerHTML = html;
  grid.querySelectorAll('[data-turno]').forEach(function(el) {
    el.addEventListener('click', function() {
      seleccionarTurno(el.dataset.turno);
    });
  });
}

// ========================
// CREAR CLIENTE EN ODOO
// ========================

function toggleCliGiro() {
  var esEmpresa = document.getElementById('cliTipo').value === 'empresa';
  document.getElementById('cliGiroGroup').style.display = esEmpresa ? 'block' : 'none';
}

function abrirIngresarVenta() {
  document.getElementById('vmTipo').value = 'Boleta';
  document.getElementById('vmOrderId').value = '';
  document.getElementById('vmCliente').value = '';
  document.getElementById('vmRut').value = '';
  document.getElementById('vmEmail').value = 'boleta@lemulux.com';
  document.getElementById('vmDireccion').value = '';
  document.getElementById('vmCiudad').value = '';
  document.getElementById('vmRegion').value = '';
  document.getElementById('vmGiro').value = '';
  document.getElementById('vmProductos').value = '';
  document.getElementById('vmError').style.display = 'none';
  toggleVmGiro();
  document.getElementById('ventaManualModal').classList.add('open');
}

function cerrarIngresarVenta() {
  document.getElementById('ventaManualModal').classList.remove('open');
}

function toggleVmGiro() {
  var esFactura = document.getElementById('vmTipo').value === 'Factura';
  document.getElementById('vmGiroGroup').style.display = esFactura ? 'block' : 'none';
}

function buildVentaManualPayload(autorizar) {
  var nombre = document.getElementById('vmCliente').value.trim();
  var rut = document.getElementById('vmRut').value.trim();
  var email = document.getElementById('vmEmail').value.trim();
  if (!nombre || !rut || !email) return null;
  var tipo = document.getElementById('vmTipo').value;
  var productosRaw = document.getElementById('vmProductos').value.trim();
  var productos = productosRaw ? productosRaw.split(String.fromCharCode(10)).map(function(l){ return l.trim(); }).filter(Boolean) : [];
  return {
    tipo: tipo,
    order_id: document.getElementById('vmOrderId').value.trim() || null,
    cliente: nombre,
    rut: rut,
    email: email,
    direccion: document.getElementById('vmDireccion').value.trim(),
    ciudad: document.getElementById('vmCiudad').value.trim(),
    region: document.getElementById('vmRegion').value,
    giro: tipo === 'Factura' ? document.getElementById('vmGiro').value.trim() : '',
    productos: productos,
    autorizar: autorizar
  };
}

function guardarVentaManual() {
  var payload = buildVentaManualPayload(false);
  var errDiv = document.getElementById('vmError');
  if (!payload) {
    errDiv.textContent = 'Nombre, RUT y email son obligatorios';
    errDiv.style.display = 'block';
    return;
  }
  errDiv.textContent = 'Guardando...';
  errDiv.style.display = 'block';
  errDiv.style.color = '#94a3b8';
  fetch('/ventas/manual', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        cerrarIngresarVenta();
        alert('Venta ingresada: ' + data.id);
        refreshData();
      } else {
        errDiv.style.color = '#f87171';
        errDiv.textContent = 'Error: ' + (data.detail || 'desconocido');
      }
    })
    .catch(function(e) {
      errDiv.style.color = '#f87171';
      errDiv.textContent = 'Error de conexion: ' + e.message;
    });
}

function guardarYAutorizarManual() {
  var payload = buildVentaManualPayload(true);
  var errDiv = document.getElementById('vmError');
  if (!payload) {
    errDiv.textContent = 'Nombre, RUT y email son obligatorios';
    errDiv.style.display = 'block';
    return;
  }
  errDiv.textContent = 'Guardando y autorizando en Odoo...';
  errDiv.style.display = 'block';
  errDiv.style.color = '#94a3b8';
  fetch('/ventas/manual', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        cerrarIngresarVenta();
        if (data.autorizado) {
          alert('Venta autorizada en Odoo: move_id=' + data.move_id);
        } else {
          alert('Venta ingresada: ' + data.id);
        }
        refreshData();
      } else {
        errDiv.style.color = '#f87171';
        errDiv.textContent = 'Error: ' + (data.detail || 'desconocido');
      }
    })
    .catch(function(e) {
      errDiv.style.color = '#f87171';
      errDiv.textContent = 'Error de conexion: ' + e.message;
    });
}

function abrirCrearCliente() {
  document.getElementById('cliNombre').value = '';
  document.getElementById('cliRut').value = '';
  document.getElementById('cliEmail').value = '';
  document.getElementById('cliDireccion').value = '';
  document.getElementById('cliCiudad').value = '';
  document.getElementById('cliRegion').value = '';
  document.getElementById('cliGiro').value = '';
  document.getElementById('cliTipo').value = 'persona';
  document.getElementById('cliError').style.display = 'none';
  toggleCliGiro();
  document.getElementById('clienteModal').classList.add('open');
}

function cerrarCrearCliente() {
  document.getElementById('clienteModal').classList.remove('open');
}

function crearClienteOdoo() {
  var nombre = document.getElementById('cliNombre').value.trim();
  var rut = document.getElementById('cliRut').value.trim();
  var email = document.getElementById('cliEmail').value.trim();
  var errDiv = document.getElementById('cliError');
  if (!nombre || !rut || !email) {
    errDiv.textContent = 'Nombre, RUT y email son obligatorios';
    errDiv.style.display = 'block';
    errDiv.style.color = '#f87171';
    return;
  }
  var esEmpresa = document.getElementById('cliTipo').value === 'empresa';
  var payload = {
    nombre: nombre,
    rut: rut,
    email: email,
    direccion: document.getElementById('cliDireccion').value.trim(),
    ciudad: document.getElementById('cliCiudad').value.trim(),
    region: document.getElementById('cliRegion').value,
    giro: esEmpresa ? document.getElementById('cliGiro').value.trim() : '',
    es_empresa: esEmpresa
  };
  errDiv.textContent = 'Creando...';
  errDiv.style.display = 'block';
  errDiv.style.color = '#94a3b8';
  fetch('/clientes/crear', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        cerrarCrearCliente();
        alert('Cliente creado en Odoo: ' + data.nombre + ' (id=' + data.partner_id + ')');
      } else {
        errDiv.style.color = '#f87171';
        errDiv.textContent = 'Error: ' + (data.detail || 'desconocido');
      }
    })
    .catch(function(e) {
      errDiv.style.color = '#f87171';
      errDiv.textContent = 'Error de conexion: ' + e.message;
    });
}

// ========================
// EVENTOS Y ARRANQUE
// ========================

document.getElementById('editModal').addEventListener('click', function(e) {
  if (e.target.id === 'editModal') closeModal();
});
document.getElementById('packModal').addEventListener('click', function(e) {
  if (e.target.id === 'packModal') closePackModal();
});
document.getElementById('calModal').addEventListener('click', function(e) {
  if (e.target.id === 'calModal') cerrarCalendario();
});
document.getElementById('clienteModal').addEventListener('click', function(e) {
  if (e.target.id === 'clienteModal') cerrarCrearCliente();
});
document.getElementById('ventaManualModal').addEventListener('click', function(e) {
  if (e.target.id === 'ventaManualModal') cerrarIngresarVenta();
});

refreshData();
setInterval(function() {
  var anyOpen = ['editModal','packModal','calModal','clienteModal'].some(function(id) {
    return document.getElementById(id).classList.contains('open');
  });
  if (!anyOpen) refreshSilente();
}, 30000);
'''





@app.get("/ui/app.js")
def ui_js():
    from fastapi.responses import Response
    return Response(content=UI_JS, media_type="application/javascript; charset=utf-8")


@app.get("/ui", response_class=HTMLResponse)
def ui_bandeja():
    return HTMLResponse(content=UI_HTML)
