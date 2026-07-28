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
import hmac
import hashlib
import base64
import html as html_module
from datetime import datetime
from typing import Optional, Any
from dataclasses import dataclass

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

ODOO_JOURNAL_FACTURA_ID = 10
ODOO_JOURNAL_BOLETA_ID = 21
ODOO_DOC_TYPE_FACTURA_ID = 1
ODOO_DOC_TYPE_BOLETA_ID = 5
ODOO_PAYMENT_TERM_CONTADO_ID = 1

# =========================
# INTERRUPTOR DE AUTO-EMISION
# =========================
# Controla si el DTE se emite solo al ingresar la compra. Valores: "auto" | "manual".
#   auto   -> boletas: se emiten siempre; facturas: solo si tienen datos completos
#   manual -> quedan pendientes para autorizar a mano
# Separado POR CANAL (mercadolibre / woocommerce / falabella) y por tipo (boletas / facturas).
# Se puede cambiar EN VIVO desde el dashboard (se guarda en la BD y persiste reinicios).
# Env vars solo dan el valor INICIAL: AUTO_EMIT_{ML|WC|FL}_{BOLETAS|FACTURAS}, y si no,
# cae al global AUTO_EMIT_{BOLETAS|FACTURAS}, y si tampoco, "manual" (arranca todo en manual).
def _auto_emit_default(src: str, tipo: str) -> str:
    esp = os.getenv(f"AUTO_EMIT_{src}_{tipo}")
    if esp:
        return esp.strip().lower()
    glob = os.getenv(f"AUTO_EMIT_{tipo}")
    if glob:
        return glob.strip().lower()
    return "manual"

AUTO_EMIT = {
    "mercadolibre": {"boletas": _auto_emit_default("ML", "BOLETAS"), "facturas": _auto_emit_default("ML", "FACTURAS")},
    "woocommerce":  {"boletas": _auto_emit_default("WC", "BOLETAS"), "facturas": _auto_emit_default("WC", "FACTURAS")},
    "falabella":    {"boletas": _auto_emit_default("FL", "BOLETAS"), "facturas": _auto_emit_default("FL", "FACTURAS")},
}

# Acciones POST-emision por canal. Valores "on" | "off" (default off; se activan a mano en el
# dashboard y persisten en la BD). Se ejecutan despues de emitir el DTE, y NUNCA tumban la
# emision (si fallan, solo se registran en el log).
#   pagar       -> registra el pago en Odoo => la factura/boleta queda PAGADA
#   email       -> envia el comprobante al cliente por el correo interno de Odoo
#   adjuntar_ml -> sube el PDF del DTE al pack de Mercado Libre (packs/fiscal_documents)
#   adjuntar_fl -> sube el PDF del DTE a Falabella Seller Center (SetInvoicePDF)
def _post_emit_default(src: str, accion: str) -> str:
    v = os.getenv(f"POST_EMIT_{src}_{accion}")
    return v.strip().lower() if v else "off"

POST_EMIT = {
    "mercadolibre": {"pagar": _post_emit_default("ML", "PAGAR"), "email": _post_emit_default("ML", "EMAIL"), "adjuntar_ml": _post_emit_default("ML", "ADJUNTAR")},
    "woocommerce":  {"pagar": _post_emit_default("WC", "PAGAR"), "email": _post_emit_default("WC", "EMAIL")},
    "falabella":    {"pagar": _post_emit_default("FL", "PAGAR"), "email": _post_emit_default("FL", "EMAIL"), "adjuntar_fl": _post_emit_default("FL", "ADJUNTAR")},
}

# Nota de Credito AUTOMATICA por canal. "on" | "off". Se separa:
#   total   -> anula la factura completa cuando la orden se cancela / devuelve ENTERA.
#   parcial -> acredita solo los items devueltos cuando la devolucion es PARCIAL (factura sigue viva).
# Default: total = on (mantiene el comportamiento previo), parcial = off (antes era solo manual).
# Env vars dan el valor INICIAL: AUTO_NC_{ML|WC|FL}_{TOTAL|PARCIAL}. Se persiste en la BD y se
# controla en vivo desde el dashboard.
def _nc_auto_default(src: str, tipo: str) -> str:
    v = os.getenv(f"AUTO_NC_{src}_{tipo}")
    if v:
        return v.strip().lower()
    return "on" if tipo == "TOTAL" else "off"

NC_AUTO = {
    "mercadolibre": {"total": _nc_auto_default("ML", "TOTAL"), "parcial": _nc_auto_default("ML", "PARCIAL")},
    "woocommerce":  {"total": _nc_auto_default("WC", "TOTAL"), "parcial": _nc_auto_default("WC", "PARCIAL")},
    "falabella":    {"total": _nc_auto_default("FL", "TOTAL"), "parcial": _nc_auto_default("FL", "PARCIAL")},
}

# Estado de folios CAF por tipo. "ok" | "agotado". Si al emitir se detecta que no hay folios,
# se DETIENE la auto-emision de ese tipo (las ventas quedan pendientes) y se avisa en el dashboard
# hasta que se carguen mas CAF y se pulse "Reanudar". Se persiste en la BD.
CAF_STATUS = {"boleta": "ok", "factura": "ok"}

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
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS nc_motivo TEXT DEFAULT ''",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS fuente TEXT DEFAULT 'mercadolibre'",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS estado_envio_real TEXT DEFAULT ''",
                "ALTER TABLE ventas ADD COLUMN IF NOT EXISTS estado_envio_sub TEXT DEFAULT ''",
            ]:
                cur.execute(stmt)
            cur.execute(
                "CREATE TABLE IF NOT EXISTS app_config (clave TEXT PRIMARY KEY, valor TEXT)"
            )
        conn.commit()
    logger.info("Tabla ventas verificada en PostgreSQL")


def wait_for_db():
    last_error = None
    for attempt in range(1, DB_RETRIES + 1):
        try:
            init_db()
            logger.info("Conexion a PostgreSQL lista")
            return True
        except Exception as e:
            last_error = e
            logger.warning(f"PostgreSQL no disponible ({attempt}/{DB_RETRIES}): {e}")
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
    forzar_actualizacion: bool = False,
):
    oid = str(pack_id) if pack_id else str(order["id"])
    if order_items_override is not None:
        order = {**order, "order_items": order_items_override}
    # Un solo fetch del shipment: tipo de envio + costo de envio del comprador (Mercado Envios).
    # El costo se persiste en order_json["shipping_cost"] para que summarize_order_items y
    # create_document lo usen sin volver a llamar a ML en cada carga.
    tipo_envio_ml, envio_costo = get_ml_shipment_info(order)
    order = {**order, "shipping_cost": envio_costo}
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
                if existing["estado"] == "enviado":
                    # Ya enviado: solo actualizar JSONs, no tocar datos tributarios
                    cur.execute(
                        "UPDATE ventas SET order_json = %s, billing_json = %s WHERE id = %s",
                        (json.dumps(order, ensure_ascii=False),
                         json.dumps(billing, ensure_ascii=False), oid),
                    )
                elif existing["estado"] == "error":
                    # En error: actualizar TODOS los campos incluyendo datos tributarios
                    cur.execute(
                        """
                        UPDATE ventas
                        SET cliente = %s, rut = %s, email = %s, direccion = %s,
                            ciudad = %s, region = %s, giro = %s, tipo_sugerido = %s,
                            order_json = %s, billing_json = %s, error = NULL
                        WHERE id = %s
                        """,
                        (
                            cliente_final, rut_final, email_final, direccion_final,
                            ciudad_final, region_final, giro_final, tipo_sugerido,
                            json.dumps(order, ensure_ascii=False),
                            json.dumps(billing, ensure_ascii=False), oid,
                        ),
                    )
                else:
                    if forzar_actualizacion:
                        cur.execute(
                            """
                            UPDATE ventas
                            SET cliente = CASE WHEN %s != '' AND %s != 'Cliente ML' THEN %s ELSE COALESCE(NULLIF(cliente,'Cliente ML'), cliente) END,
                                rut = CASE WHEN %s != '' THEN %s ELSE rut END,
                                email = CASE WHEN (email IS NULL OR email = '') THEN %s ELSE email END,
                                direccion = CASE WHEN %s != '' THEN %s ELSE direccion END,
                                ciudad = CASE WHEN %s != '' THEN %s ELSE ciudad END,
                                region = CASE WHEN %s != '' THEN %s ELSE region END,
                                tipo_sugerido = %s,
                                order_json = %s,
                                billing_json = %s,
                                giro = CASE WHEN %s != '' AND %s != '(boleta)' THEN %s
                                            WHEN (giro IS NULL OR giro = '') THEN %s
                                            ELSE giro END
                            WHERE id = %s
                            """,
                            (
                                cliente_final, cliente_final, cliente_final,
                                rut_final, rut_final,
                                email_final,
                                direccion_final, direccion_final,
                                ciudad_final, ciudad_final,
                                region_final, region_final,
                                tipo_sugerido,
                                json.dumps(order, ensure_ascii=False),
                                json.dumps(billing, ensure_ascii=False),
                                giro_final, giro_final, giro_final, giro_final,
                                oid,
                            ),
                        )
                    else:
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
                                cliente_final, rut_final, email_final, direccion_final,
                                ciudad_final, region_final, tipo_sugerido,
                                json.dumps(order, ensure_ascii=False),
                                json.dumps(billing, ensure_ascii=False),
                                giro_final, oid,
                            ),
                        )
                conn.commit()
                logger.info(f"[{oid}] Venta actualizada (estado={existing['estado']})")
                return

            cur.execute(
                """
                INSERT INTO ventas
                    (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region,
                     tipo_sugerido, estado, estado_envio, order_json, billing_json, tipo_envio_ml, fuente)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s, %s, 'mercadolibre')
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    oid, pack_id or None, cliente_final, rut_final, email_final,
                    giro_final, direccion_final, ciudad_final, region_final, tipo_sugerido,
                    order.get("status", "paid"),
                    json.dumps(order, ensure_ascii=False),
                    json.dumps(billing, ensure_ascii=False),
                    tipo_envio_ml,
                ),
            )
        conn.commit()
    logger.info(f"[{oid}] Guardada -> tipo={tipo_sugerido} rut={rut_final or 'sin RUT'} cliente={cliente_final}")


def list_ventas(estado: Optional[str] = None, ids: Optional[list] = None,
                desde: Optional[str] = None, limit: Optional[int] = None) -> list:
    """Lista ventas. Los filtros (ids / desde / limit) se aplican en SQL para NO
    traer las ~6200 filas con su order_json completo (payload de ~4.6MB y mucho
    CPU de parseo) cuando el llamador solo necesita unas pocas."""
    cols = "id, cliente, rut, email, giro, direccion, ciudad, region, tipo_sugerido, estado, estado_envio, estado_envio_real, estado_envio_sub, pack_id, move_id, partner_id, error, creado_en, enviado_en, order_json, tipo_envio_ml, fuente"
    where, params = [], []
    if estado:
        where.append("estado = %s"); params.append(estado)
    if ids:
        where.append("(id = ANY(%s::text[]) OR pack_id = ANY(%s::text[]))")
        params.extend([list(ids), list(ids)])
    if desde:
        where.append("creado_en >= %s"); params.append(desde)
    sql = f"SELECT {cols} FROM ventas"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY creado_en DESC"
    if limit:
        sql += " LIMIT %s"; params.append(int(limit))
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
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


def get_config(clave: str, default: str = None) -> Optional[str]:
    """Lee un valor de configuracion persistente (tabla app_config)."""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT valor FROM app_config WHERE clave = %s", (clave,))
                row = cur.fetchone()
        return row["valor"] if row else default
    except Exception as e:
        logger.warning(f"get_config({clave}) fallo: {e}")
        return default


def set_config(clave: str, valor: str):
    """Guarda/actualiza un valor de configuracion persistente."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app_config (clave, valor) VALUES (%s, %s)
                   ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor""",
                (clave, valor),
            )
        conn.commit()


# -- Token de refresco ML: persistencia DURABLE (sobrevive a los redeploys) --
# ML rota el refresh_token en cada uso y el proceso guardaba el nuevo SOLO en
# os.environ (efimero en Railway) -> un redeploy arrancaba con el token original
# ya consumido y ML dejaba de autenticar. Ahora lo persistimos en app_config.
def get_ml_refresh_token() -> str:
    tok = get_config("ml_refresh_token")
    if tok:
        return tok
    return get_env("ML_REFRESH_TOKEN", required=False)  # fallback: primer arranque


def set_ml_refresh_token(tok: str):
    if not tok:
        return
    os.environ["ML_REFRESH_TOKEN"] = tok          # proceso actual
    try:
        set_config("ml_refresh_token", tok)       # durable, sobrevive redeploys
    except Exception as e:
        logger.error(f"No se pudo persistir ml_refresh_token: {e}")


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
# EXTRACCION ML
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
    # Nombre del RECIBO (persona/boleta): ML lo entrega en additional_info FIRST_NAME + LAST_NAME.
    # Es el "dato para su recibo de cobro" que ingreso el comprador, y puede diferir del nombre
    # de la cuenta. Debe primar por sobre el nombre de la cuenta.
    ai_fields = {}
    for item in billing_info.get("additional_info") or []:
        t = (item.get("type") or "").upper()
        v = item.get("value")
        if isinstance(v, str) and v.strip():
            ai_fields[t] = v.strip()
    recibo_nombre = " ".join(x for x in [ai_fields.get("FIRST_NAME", ""), ai_fields.get("LAST_NAME", "")] if x).strip()
    if recibo_nombre:
        return recibo_nombre
    name = (billing_info.get("name") or "").strip()
    last_name = (billing_info.get("last_name") or "").strip()
    if name and last_name:
        return f"{name} {last_name}"
    if name:
        return name
    buyer = order.get("buyer") or {}
    full_name = " ".join(
        x for x in [buyer.get("first_name", "").strip(), buyer.get("last_name", "").strip()] if x
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
    street = (address.get("street_name") or address.get("address_line") or address.get("line") or "")
    number = address.get("street_number") or address.get("number") or ""
    comment = address.get("comment") or address.get("reference") or ""
    municipality = (address.get("municipality_name") or safe_get(address, "municipality", "name", default=""))
    city = (address.get("city_name") or safe_get(address, "city", "name", default=""))
    state = (address.get("state_name") or safe_get(address, "state", "name", default=""))
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


def flatten_strings(obj) -> list:
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
    banned = ["rut", "giro", "actividad", "economic activity", "razon social",
              "razon social", "business name", "company name", "boleta", "factura", "consumidor final"]
    if any(b in tl for b in banned):
        return False
    has_number = bool(re.search(r"\b\d{2,5}\b", t))
    has_street_word = bool(re.search(
        r"\b(calle|av\.?|avenida|pasaje|camino|ruta|general|manuel|los|las|el|la|encomenderos|montt|ohiggins|vicuna|providencia|apoquindo)\b", tl))
    has_location_hint = bool(re.search(
        r"\b(santiago|las condes|providencia|maipu|maipu|nunoa|nunoa|coronel|biobio|metropolitana|valparaiso|valparaiso|concepcion|concepcion|temuco|antofagasta)\b", tl))
    return has_number and (has_street_word or has_location_hint)


def score_address_candidate(text: str) -> int:
    t = text.lower()
    score = 0
    if re.search(r"\b\d{2,5}\b", t):
        score += 3
    if re.search(r"\b(of|oficina|depto|departamento|casa|piso|torre|edif|edificio)\b", t):
        score += 3
    if re.search(r"\b(santiago|las condes|providencia|coronel|biobio|metropolitana)\b", t):
        score += 3
    if re.search(r"\b(calle|av|avenida|pasaje|camino|ruta|general|manuel|encomenderos)\b", t):
        score += 2
    return score


ML_STATE_CODE_TO_REGION = {
    "CL-AI": "Aysen del Gral. Carlos Ibanez del Campo",
    "CL-AN": "Antofagasta",
    "CL-AP": "Arica y Parinacota",
    "CL-AR": "de la Araucania",
    "CL-AT": "Atacama",
    "CL-BI": "del BioBio",
    "CL-CO": "Coquimbo",
    "CL-LI": "del Libertador Gral. Bernardo O'Higgins",
    "CL-LL": "de los Lagos",
    "CL-LR": "Los Rios",
    "CL-MA": "Magallanes",
    "CL-ML": "del Maule",
    "CL-NB": "del Nuble",
    "CL-RM": "Metropolitana",
    "CL-TA": "Tarapaca",
    "CL-VS": "Valparaiso",
}


def extract_from_additional_info(billing_info: dict) -> dict:
    fields = {}
    for item in billing_info.get("additional_info") or []:
        t = (item.get("type") or "").upper()
        v = item.get("value")
        if isinstance(v, str) and v.strip():
            fields[t] = v.strip()
    return fields


def extract_direccion_from_additional_info(billing_info: dict) -> str:
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
    fields = extract_from_additional_info(billing_info)
    return (
        fields.get("CITY_NAME") or fields.get("NEIGHBORHOOD") or
        safe_get(billing_info, "address", "city_name", default="") or ""
    )


def extract_region_from_billing(billing_info: dict) -> str:
    fields = extract_from_additional_info(billing_info)
    state_code = fields.get("STATE_CODE", "").upper()
    if state_code and state_code in ML_STATE_CODE_TO_REGION:
        return ML_STATE_CODE_TO_REGION[state_code]
    return (
        fields.get("STATE_NAME") or
        safe_get(billing_info, "address", "state_name", default="") or ""
    )


def enrich_from_shipment(order: dict, rut: str, cliente: str, direccion: str, ciudad: str, region: str):
    """Cuando billing_info viene vacio, completa datos faltantes desde el receiver_address del shipment."""
    shipping = order.get("shipping") or {}
    shipping_id = shipping.get("id")
    if not shipping_id:
        return rut, cliente, direccion, ciudad, region
    try:
        shipment = get_ml_shipment(shipping_id)
        ra = shipment.get("receiver_address") or {}
        if not cliente or cliente == "Cliente ML":
            receiver_name = ra.get("receiver_name") or ""
            if receiver_name:
                cliente = receiver_name.title()
        if not direccion:
            addr = ra.get("address_line") or ""
            if addr:
                direccion = addr
        if not ciudad:
            ciudad = (ra.get("city") or {}).get("name") or ""
        if not region:
            state_id = (ra.get("state") or {}).get("id") or ""
            if state_id and state_id in ML_STATE_CODE_TO_REGION:
                region = ML_STATE_CODE_TO_REGION[state_id]
            else:
                region = (ra.get("state") or {}).get("name") or ""
    except Exception as e:
        logger.warning(f"enrich_from_shipment: no se pudo obtener shipment {shipping_id}: {e}")
    return rut, cliente, direccion, ciudad, region


def extract_direccion(order: dict, billing_info: dict, billing_raw: dict = None) -> str:
    direccion = extract_direccion_from_additional_info(billing_info)
    if direccion:
        return direccion
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
    taxpayer_desc = (safe_get(billing_info, "taxes", "taxpayer_type", "description", default="") or "").strip().lower()
    if taxpayer_desc and any(x in taxpayer_desc for x in ["responsable", "empresa", "negocio", "iva", "juridica"]):
        return "Factura"
    if extract_activity(billing_info, order):
        return "Factura"
    for key in ("business_name", "company_name", "razon_social", "social_reason", "legal_name"):
        val = billing_info.get(key)
        if isinstance(val, str) and val.strip():
            return "Factura"
    nombre = extract_name(billing_info, order) or ""
    nombre_upper = nombre.upper()
    empresa_keywords = [" SPA", " LTDA", "S.A.", " SA ", "LIMITADA", "EIRL", " INC",
                        " CORP", "COMERCIAL ", "CONSTRUCTORA", "CONSULTORA",
                        "INVERSIONES", "HOLDING", "SOCIEDAD ", " CIA", " CIA."]
    if any(kw in nombre_upper for kw in empresa_keywords):
        return "Factura"
    return "Boleta"


def extract_shipping_cost(order: dict) -> float:
    """Costo de envio BRUTO que pago el comprador. Mercado Libre lo trae en
    payments[].shipping_cost (con fallback al shipping_cost raiz). WC/FL/manual no
    traen payments, asi que aqui dan 0 (WC ya incluye su envio en order_items)."""
    envio = 0.0
    for p in (order.get("payments") or []):
        try:
            envio += float(p.get("shipping_cost") or 0)
        except (TypeError, ValueError):
            pass
    if envio <= 0:
        try:
            envio = float(order.get("shipping_cost") or 0)
        except (TypeError, ValueError):
            envio = 0.0
    return envio if envio > 0 else 0.0


def extract_telefono(order: dict, billing_info: dict = None) -> str:
    """Telefono del cliente para grabar en el partner de Odoo. Busca (en orden):
    order.telefono/phone (FL fake_order / manual), buyer.phone (ML puede traer dict
    {area_code, number}), y billing_info (additional_info tipo PHONE, o claves phone/telefono)."""
    billing_info = billing_info or {}
    for key in ("telefono", "phone"):
        v = str(order.get(key) or "").strip()
        if v:
            return v
    bp = safe_get(order, "buyer", "phone", default=None)
    if isinstance(bp, dict):
        num = str(bp.get("number") or "").strip()
        area = str(bp.get("area_code") or "").strip()
        if num:
            return (area + num).strip()
    elif bp:
        v = str(bp).strip()
        if v:
            return v
    for item in billing_info.get("additional_info") or []:
        k = str(item.get("type") or item.get("id") or "").lower()
        if "phone" in k or "telefono" in k:
            v = str(item.get("value") or "").strip()
            if v:
                return v
    for key in ("phone", "telephone", "telefono"):
        v = str(billing_info.get(key) or "").strip()
        if v:
            return v
    return ""


def summarize_order_items(order: dict) -> tuple:
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
    envio = extract_shipping_cost(order)
    if envio > 0:
        total_bruto += envio
        items_summary.append(f"Despacho ({int(round(envio))})")
    return items_summary, item_count, round(total_bruto, 2)


# =========================
# MERCADO LIBRE API
# =========================

def ml_headers():
    return {"Authorization": f"Bearer {get_env('ML_ACCESS_TOKEN')}"}


_ml_scope = ""


def refresh_ml_token() -> bool:
    global _ml_scope
    try:
        payload = {
            "grant_type": "refresh_token",
            "client_id": get_env("ML_CLIENT_ID"),
            "client_secret": get_env("ML_CLIENT_SECRET"),
            "refresh_token": get_ml_refresh_token(),
        }
        res = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
        res.raise_for_status()
        data = res.json()
        if data.get("access_token"):
            os.environ["ML_ACCESS_TOKEN"] = data["access_token"]
        if data.get("refresh_token"):
            set_ml_refresh_token(data["refresh_token"])
        if data.get("scope"):
            _ml_scope = data["scope"]
        logger.info(f"Token ML renovado (scope: {_ml_scope or 'desconocido'})")
        return True
    except Exception as e:
        logger.error(f"Error renovando token ML: {e}")
        return False


def schedule_token_refresh():
    while True:
        time.sleep(TOKEN_REFRESH_INTERVAL)
        logger.info("Renovacion programada del token ML...")
        refresh_ml_token()


_consecutive_429 = 0
_ml_lock = threading.Lock()   # serializa todos los requests a ML entre threads
_ml_last_request = 0.0        # timestamp del ultimo request exitoso


def ml_get(url: str, extra_headers: dict = None) -> dict:
    global _consecutive_429, _ml_last_request
    with _ml_lock:
        # Respetar minimo 500ms entre requests para no saturar el rate limit
        elapsed = time.time() - _ml_last_request
        min_gap = 0.5 + min(_consecutive_429 * 0.5, 5.0)
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)

        for attempt in range(6):
            try:
                _h = ml_headers()
                if extra_headers:
                    _h = {**_h, **extra_headers}
                res = requests.get(url, headers=_h, timeout=30)
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
                wait = min(10 * (attempt + 1) + (_consecutive_429 * 3), 90)
                logger.warning(f"429 en {url}, esperando {wait}s (consecutivos: {_consecutive_429})")
                time.sleep(wait)
                continue

            if res.status_code == 404:
                _consecutive_429 = 0
                _ml_last_request = time.time()
                return {}

            res.raise_for_status()
            _consecutive_429 = 0
            _ml_last_request = time.time()
            return res.json()

        raise Exception(f"ML devolvio demasiados errores para {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_shipment(shipping_id) -> dict:
    if not shipping_id:
        return {}
    try:
        return ml_get(f"https://api.mercadolibre.com/shipments/{shipping_id}")
    except Exception:
        return {}


def get_ml_shipment_info(order: dict) -> tuple:
    """Con UN solo fetch del shipment devuelve (tipo_envio, costo_envio_comprador).
    El costo que paga el comprador (Mercado Envios) esta en shipping_option.cost — 0.0 si es
    gratis. Confirmado en docs ML: en shipping_option, 'cost' = costo real a cargo del comprador
    (list_cost = precio de lista). No esta en order.payments; por eso hay que leer el shipment."""
    shipping = order.get("shipping") or {}
    shipping_id = shipping.get("id")
    if not shipping_id:
        return "", 0.0
    shipment = get_ml_shipment(shipping_id)
    if not shipment:
        return "", 0.0
    logistic_type = shipment.get("logistic_type", "")
    if logistic_type == "fulfillment":
        tipo = "Full"
    else:
        sender_types = (shipment.get("sender_address") or {}).get("types") or []
        if "self_service_partner" in sender_types:
            tipo = "Flex"
        elif "milkrun" in sender_types or logistic_type == "cross_docking":
            tipo = "Colecta"
        else:
            tipo = logistic_type or ""
    try:
        costo = float((shipment.get("shipping_option") or {}).get("cost") or 0)
    except (TypeError, ValueError):
        costo = 0.0
    return tipo, (costo if costo > 0 else 0.0)


def extract_logistic_type(order: dict) -> str:
    return get_ml_shipment_info(order)[0]


def get_ml_pack(pack_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/packs/{pack_id}")


def get_venta_by_pack(pack_id: str) -> Optional[dict]:
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM ventas WHERE pack_id = %s LIMIT 1", (str(pack_id),))
            row = cur.fetchone()
    return dict(row) if row else None


def merge_order_items(orders: list) -> list:
    all_items = []
    for order in orders:
        all_items.extend(order.get("order_items", []))
    return all_items


def get_ml_billing_raw(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}/billing_info")


def get_ml_billing_raw_safe(order_id: str) -> dict:
    """Obtiene billing_info con reintento automatico si viene vacio (ej: tras renovacion de token)."""
    billing = get_ml_billing_raw(order_id)
    if not billing or billing == {}:
        logger.warning(f"[{order_id}] billing_info vacio, reintentando en 3s...")
        time.sleep(3)
        billing = get_ml_billing_raw(order_id)
    if not billing or billing == {}:
        logger.warning(f"[{order_id}] billing_info sigue vacio tras reintento")
    return billing


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


def get_partner_fields(ctx: OdooCtx) -> set:
    fields = odoo_exec(ctx, "res.partner", "fields_get", [], {"attributes": ["string"]})
    return set(fields.keys())


def get_move_fields(ctx: OdooCtx) -> set:
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
    ids = odoo_exec(ctx, "account.tax", "search",
        [[["type_tax_use", "=", "sale"], ["amount", "=", 19], ["active", "=", True]]], {"limit": 1})
    return ids[0] if ids else None


def get_activity_field_name(ctx: OdooCtx) -> Optional[str]:
    fields = get_partner_fields(ctx)
    for candidate in ("l10n_cl_activity_description", "activity_description", "x_studio_giro", "x_giro", "giro"):
        if candidate in fields:
            return candidate
    return None


def rut_con_guion(rut_norm: str) -> str:
    if not rut_norm or len(rut_norm) < 2:
        return rut_norm
    return rut_norm[:-1] + "-" + rut_norm[-1]


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut_norm = normalize_rut(rut)
    if not rut_norm:
        return None
    rut_odoo = rut_con_guion(rut_norm)
    ids = odoo_exec(ctx, "res.partner", "search", [[["vat", "=", rut_odoo]]], {"limit": 1})
    return ids[0] if ids else None


def find_partner_by_ml_order(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(ctx, "res.partner", "search",
        [[["comment", "ilike", f"ML_ORDER:{order_id}"]]], {"limit": 1})
    return ids[0] if ids else None


def get_state_id(ctx: OdooCtx, region_name: str) -> Optional[int]:
    if not region_name:
        return None
    ids = odoo_exec(ctx, "res.country.state", "search",
        [[["name", "ilike", region_name], ["country_id.code", "=", "CL"]]], {"limit": 1})
    return ids[0] if ids else None


def upsert_partner(ctx, order_id, nombre, rut, email, giro, direccion, es_empresa, tipo, ciudad="", region="", telefono="") -> int:
    partner_fields = get_partner_fields(ctx)
    activity_field = get_activity_field_name(ctx)
    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_id_type(ctx)
    taxpayer_type = "1" if es_empresa else "4"
    rut_odoo = rut_con_guion(normalize_rut(rut)) if rut else ""
    partner_id = find_partner_by_rut(ctx, rut) or find_partner_by_ml_order(ctx, order_id)
    logger.info(f"upsert_partner: rut_odoo='{rut_odoo}' -> partner_id={'encontrado:' + str(partner_id) if partner_id else 'no encontrado'}")
    giro_a_usar = giro.strip()
    if tipo == "Boleta" and not giro_a_usar:
        giro_a_usar = DEFAULT_BOLETA_ACTIVITY
    vals = {"name": nombre, "comment": f"ML_ORDER:{order_id}"}
    # Solo grabar email si hay uno real; si viene vacio se deja el partner sin correo
    # (no queremos escribir boleta@lemulux.com, que recibimos nosotros).
    if email:
        vals["email"] = email
    if rut:
        vals["vat"] = rut_con_guion(rut)
    if email and "l10n_cl_dte_email" in partner_fields:
        vals["l10n_cl_dte_email"] = email
    if direccion and "street" in partner_fields:
        vals["street"] = direccion
    if ciudad and "city" in partner_fields:
        vals["city"] = ciudad
    if telefono:
        tel = str(telefono).strip()
        if "phone" in partner_fields:
            vals["phone"] = tel
        # Numeros moviles chilenos (9 xxxx xxxx) tambien al campo Movil si existe.
        if "mobile" in partner_fields and re.sub(r"\D", "", tel).lstrip("56").startswith("9"):
            vals["mobile"] = tel
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
        logger.info(f"Partner actualizado: id={partner_id}")
        return partner_id
    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado: id={partner_id} empresa={es_empresa}")
    return partner_id


# =========================
# CREACION DOCUMENTO ODOO
# =========================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    # Solo busca documentos activos (draft o posted), NO cancelados
    # Asi anular_venta() + autorizar() crea documento nuevo correctamente
    ids = odoo_exec(ctx, "account.move", "search",
        [[["ref", "=", str(order_id)], ["state", "in", ["draft", "posted"]]]], {"limit": 1})
    return ids[0] if ids else None


def create_document(order, billing_raw, tipo, email, giro,
                    cliente_override=None, rut_override=None, direccion_override=None,
                    ciudad_override=None, region_override=None) -> tuple:
    ctx = odoo_connect()
    order_id = str(order["id"])
    # Referencia del cliente / clave de deduplicacion.
    # Falabella: usar el Nro de orden VISIBLE (OrderNumber, ej 3242572329), no el OrderId
    # interno (FL-1159267658). Es el numero que vemos en Seller Center y con el que se evita
    # duplicar documentos. ML/WC/manual conservan el order_id como referencia.
    order_number = str(order.get("order_number") or "").strip()
    doc_ref = order_number if (order_id.startswith("FL-") and order_number) else order_id
    existing = find_existing_move(ctx, doc_ref)
    if existing:
        # Documento ya emitido para esta orden -> NO se duplica. Se devuelve el existente para
        # que el flujo siga con la carga del PDF / envio de correo segun corresponda.
        logger.info(f"[{order_id}] Documento ya existe (ref={doc_ref}): move_id={existing}, no se duplica")
        return existing, 0
    billing_info = get_billing_info(billing_raw)
    rut = normalize_rut(rut_override) if rut_override else extract_rut(billing_info)
    nombre = (cliente_override or extract_name(billing_info, order) or "Cliente ML").strip()
    direccion = (direccion_override or extract_direccion(order, billing_info, billing_raw) or "").strip()
    ciudad = (ciudad_override or extract_ciudad_from_billing(billing_info) or "").strip()
    region = (region_override or extract_region_from_billing(billing_info) or "").strip()
    email = (email or "").strip()
    # Default de correo SOLO para ML/WC/manual. En Falabella, sin email real se deja VACIO
    # (boleta@lemulux.com lo recibimos nosotros; no queremos auto-enviarnoslo).
    if not email and not order_id.startswith("FL-"):
        email = ML_DEFAULT_EMAIL
    giro = (giro or "").strip()
    telefono = extract_telefono(order, billing_info)
    es_empresa = tipo == "Factura"
    if tipo == "Factura" and (not nombre or not rut or not direccion or not giro):
        raise Exception("Para Factura se requiere razon social, RUT, direccion y giro")
    if tipo == "Boleta" and not giro:
        giro = DEFAULT_BOLETA_ACTIVITY
    partner_id = upsert_partner(ctx=ctx, order_id=order_id, nombre=nombre, rut=rut,
                                email=email, giro=giro, direccion=direccion,
                                es_empresa=es_empresa, tipo=tipo, ciudad=ciudad, region=region,
                                telefono=telefono)
    journal_id = get_journal(ctx, tipo)
    if not journal_id:
        raise Exception(f"No se encontro diario Odoo para {tipo}")
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
    # Envio Mercado Libre: el costo que pago el comprador (payments[].shipping_cost) se agrega
    # como una linea mas en BRUTO; create_document la netea igual que los productos (Odoo guarda
    # neto). Envio gratis = 0 -> no agrega nada. WC/FL/manual no traen payments (WC mete su envio
    # por otra via en wc_build_order_items). Mismo helper que summarize_order_items -> preview = DTE.
    envio_bruto = extract_shipping_cost(order)
    if envio_bruto > 0:
        price_envio = round(envio_bruto / IVA_RATE, 2)
        envio_line = {"name": "Despacho", "quantity": 1, "price_unit": price_envio}
        if tax_id:
            envio_line["tax_ids"] = [(6, 0, [tax_id])]
        lines.append((0, 0, envio_line))
        logger.info(f"[{order_id}] Linea de envio agregada: bruto={envio_bruto} neto={price_envio}")
    if not lines:
        raise Exception("La orden no tiene lineas")
    move_vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "partner_shipping_id": partner_id,
        "ref": str(doc_ref),
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
    logger.info(f"[{order_id}] Documento creado: move_id={move_id} tipo={tipo}")
    return move_id, partner_id


# =========================
# EMISION / NC / AUTONOMIA
# =========================

def datos_completos_para_factura(venta: dict) -> bool:
    """Una factura solo se auto-emite si tiene razon social, RUT, direccion y giro reales."""
    cliente = (venta.get("cliente") or "").strip()
    rut = (venta.get("rut") or "").strip()
    direccion = (venta.get("direccion") or "").strip()
    giro = (venta.get("giro") or "").strip()
    cliente_ok = bool(cliente) and not cliente.startswith("Cliente ")
    giro_ok = bool(giro) and giro != DEFAULT_BOLETA_ACTIVITY
    return cliente_ok and bool(rut) and bool(direccion) and giro_ok


def registrar_pago_odoo(move_id: int):
    """Registra el pago de una factura/boleta en Odoo => queda PAGADA.
    Usa el wizard account.payment.register (estable en Odoo 15-18). Idempotente.
    Opcional: ODOO_PAYMENT_JOURNAL_ID para forzar el diario de pago (banco/caja)."""
    from datetime import date
    ctx = odoo_connect()
    mv = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": ["state", "payment_state"]})
    if not mv:
        raise Exception(f"Documento {move_id} no encontrado en Odoo")
    mv = mv[0]
    if mv.get("state") != "posted":
        raise Exception(f"Documento {move_id} no esta publicado (estado {mv.get('state')})")
    if mv.get("payment_state") in ("paid", "in_payment", "reversed"):
        return  # ya pagada / no aplica
    vals = {"payment_date": date.today().isoformat()}
    journal_id = None
    jid = os.getenv("ODOO_PAYMENT_JOURNAL_ID")
    if jid:
        try:
            journal_id = int(jid)
        except ValueError:
            journal_id = None
    if not journal_id:
        # Sin override, usar el diario de BANCO (el pago se registra por banco)
        banks = odoo_exec(ctx, "account.journal", "search", [[["type", "=", "bank"]]], {"limit": 1})
        if banks:
            journal_id = banks[0]
    if journal_id:
        vals["journal_id"] = journal_id
    wizard_id = odoo_exec(ctx, "account.payment.register", "create", [vals],
                          {"context": {"active_model": "account.move", "active_ids": [move_id]}})
    odoo_exec(ctx, "account.payment.register", "action_create_payments", [[wizard_id]])
    logger.info(f"Pago registrado en Odoo (move_id={move_id}) -> factura PAGADA")


def enviar_comprobante_email(move_id: int):
    """Envia el comprobante (con su PDF) al cliente por el correo interno de Odoo.
    Usa la plantilla estandar de factura si existe; si no, la primera de account.move.
    Si el cliente NO tiene email, NO envia (evita auto-enviarnos boleta@lemulux.com)."""
    ctx = odoo_connect()
    # Verificar que el partner del documento tenga email real antes de enviar.
    try:
        mv = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": ["partner_id"]})
        partner_id = mv[0]["partner_id"][0] if mv and mv[0].get("partner_id") else None
        correo = ""
        if partner_id:
            pr = odoo_exec(ctx, "res.partner", "read", [[partner_id]], {"fields": ["email"]})
            correo = (pr[0].get("email") or "").strip() if pr else ""
        if not correo or correo.lower() == ML_DEFAULT_EMAIL.lower():
            logger.info(f"[email] move_id={move_id} sin email real del cliente -> no se envia")
            return
    except Exception as e:
        logger.warning(f"[email] no se pudo verificar email del cliente (move_id={move_id}): {e}")
        return
    tmpl_id = None
    try:
        ref = odoo_exec(ctx, "ir.model.data", "check_object_reference",
                        ["account", "email_template_edi_invoice"])
        if ref and len(ref) == 2:
            tmpl_id = ref[1]
    except Exception:
        tmpl_id = None
    if not tmpl_id:
        found = odoo_exec(ctx, "mail.template", "search", [[["model", "=", "account.move"]]], {"limit": 1})
        tmpl_id = found[0] if found else None
    if not tmpl_id:
        raise Exception("No hay plantilla de correo de factura configurada en Odoo")
    odoo_exec(ctx, "mail.template", "send_mail", [[tmpl_id], move_id], {"force_send": True})
    logger.info(f"Comprobante enviado por email desde Odoo (move_id={move_id})")


def obtener_pdf_dte_odoo(move_id: int) -> bytes:
    """Obtiene el PDF del DTE (factura/boleta) desde Odoo.
    1) Intenta leer un PDF ya ADJUNTO al documento via XML-RPC (usa el API key, sin password).
    2) Si no hay, lo genera via el controlador de reportes con una sesion web
       (requiere ODOO_PASSWORD real; el API key no sirve para el login web)."""
    # --- 1) PDF adjunto (XML-RPC) ---
    try:
        ctx = odoo_connect()
        atts = odoo_exec(
            ctx, "ir.attachment", "search_read",
            [[["res_model", "=", "account.move"], ["res_id", "=", move_id],
              ["mimetype", "=", "application/pdf"]]],
            {"fields": ["datas"], "limit": 1, "order": "id desc"},
        )
        if atts and atts[0].get("datas"):
            pdf = base64.b64decode(atts[0]["datas"])
            if pdf[:4] == b"%PDF":
                logger.info(f"PDF DTE obtenido de adjunto Odoo (move_id={move_id}, {len(pdf)} bytes)")
                return pdf
    except Exception as e:
        logger.warning(f"obtener_pdf_dte_odoo: sin PDF adjunto o fallo lectura ({e}); intento sesion web")

    # --- 2) Generar via controlador de reportes (sesion web) ---
    url = get_env("ODOO_URL").rstrip("/")
    db = get_env("ODOO_DB")
    user = get_env("ODOO_USER")
    pwd = os.getenv("ODOO_PASSWORD") or get_env("ODOO_API_KEY")
    report = os.getenv("ODOO_INVOICE_REPORT", "account.account_invoices")
    sess = requests.Session()
    auth = sess.post(f"{url}/web/session/authenticate",
                     json={"jsonrpc": "2.0", "params": {"db": db, "login": user, "password": pwd}},
                     timeout=30)
    auth.raise_for_status()
    if not (auth.json().get("result") or {}).get("uid"):
        raise Exception("No se pudo autenticar la sesion web de Odoo para el PDF (setear ODOO_PASSWORD con la clave real)")
    r = sess.get(f"{url}/report/pdf/{report}/{move_id}", timeout=90)
    r.raise_for_status()
    if not r.content.startswith(b"%PDF"):
        raise Exception("Odoo no devolvio un PDF valido (revisar ODOO_INVOICE_REPORT)")
    return r.content


def obtener_datos_dte_odoo(move_id: int) -> dict:
    """Lee del documento en Odoo el folio (numero) y la fecha del DTE ya emitido.
    Falabella exige invoiceNumber + invoiceDate al cargar el documento tributario.
    - numero: usa el folio de la localizacion chilena (l10n_latam_document_number);
      cae a 'name' si no existe.
    - fecha:  invoice_date (fecha del documento) o 'date' contable."""
    ctx = odoo_connect()
    fields = ["name", "invoice_date", "date"]
    move_fields = odoo_exec(ctx, "account.move", "fields_get", [], {"attributes": ["string"]})
    if "l10n_latam_document_number" in move_fields:
        fields.insert(0, "l10n_latam_document_number")
    rows = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": fields})
    if not rows:
        raise Exception(f"Documento {move_id} no encontrado en Odoo")
    mv = rows[0]
    folio = (mv.get("l10n_latam_document_number") or "").strip()
    if folio:
        # El folio de l10n_cl ya es el numero puro del DTE; usarlo tal cual.
        numero = folio
    else:
        # Fallback: 'name' suele traer prefijo (ej "FAC/2026/000123"); tomar el ultimo grupo de digitos.
        name = (mv.get("name") or "").strip()
        grupos = re.findall(r"\d+", name)
        numero = grupos[-1] if grupos else name
    fecha = mv.get("invoice_date") or mv.get("date") or datetime.now().strftime("%Y-%m-%d")
    return {"numero": numero, "fecha": str(fecha)[:10]}


def obtener_lineas_dte_odoo(move_id: int) -> list:
    """Lee las lineas de PRODUCTO del documento (factura/boleta) para armar la NC parcial.
    Devuelve [{line_index, name, quantity, price_unit, price_subtotal}] en el orden del DTE."""
    ctx = odoo_connect()
    mv = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": ["invoice_line_ids"]})
    if not mv:
        return []
    line_ids = mv[0].get("invoice_line_ids") or []
    if not line_ids:
        return []
    lines = odoo_exec(ctx, "account.move.line", "read", [line_ids],
                      {"fields": ["name", "quantity", "price_unit", "price_subtotal", "display_type"]})
    out = []
    idx = 0
    for ln in lines:
        # Solo lineas de producto (excluir secciones/notas)
        if ln.get("display_type") not in (False, None, "product"):
            continue
        out.append({
            "line_index": idx,
            "line_id": ln["id"],
            "name": ln.get("name") or "",
            "quantity": ln.get("quantity") or 0,
            "price_unit": ln.get("price_unit") or 0,
            "price_subtotal": ln.get("price_subtotal") or 0,
        })
        idx += 1
    return out


def subir_comprobante_ml(pack_id: str, pdf_bytes: bytes, oid: str) -> dict:
    """Sube el PDF del comprobante al pack de Mercado Libre.
    POST /packs/{pack_id}/fiscal_documents (multipart, campo 'fiscal_document', PDF <= 1MB)."""
    if not pdf_bytes:
        raise Exception("PDF vacio")
    if len(pdf_bytes) > 1024 * 1024:
        raise Exception(f"El PDF pesa {len(pdf_bytes)} bytes (> 1MB); ML no lo acepta")
    api_url = f"https://api.mercadolibre.com/packs/{pack_id}/fiscal_documents"
    files = {"fiscal_document": (f"comprobante_{oid}.pdf", pdf_bytes, "application/pdf")}
    res = requests.post(api_url, headers=ml_headers(), files=files, timeout=120)
    if res.status_code == 401:
        refresh_ml_token()
        res = requests.post(api_url, headers=ml_headers(), files=files, timeout=120)
    # 409 = el pack YA tiene un comprobante cargado -> idempotente, lo tratamos como OK.
    if res.status_code == 409:
        logger.info(f"[{oid}] ML 409 en pack {pack_id}: el comprobante YA estaba cargado (ok idempotente)")
        return {"ok": True, "ya_cargado": True, "status_code": 409}
    res.raise_for_status()
    try:
        return res.json()
    except Exception:
        return {"ok": True, "status_code": res.status_code}


def adjuntar_comprobante_ml(oid: str, move_id: int):
    """Sube a Mercado Libre el PDF del DTE ya emitido en Odoo. Solo ML.
    Omite logistica Full (fulfillment), que MLC no permite adjuntar comprobante."""
    venta = get_venta(oid)
    if not venta:
        raise Exception("Venta no encontrada")
    if (venta.get("tipo_envio_ml") or "") == "Full":
        logger.info(f"[{oid}] Adjuntar ML omitido: logistica Full (fulfillment) no permite subir comprobante en MLC")
        return {"omitido": "logistica Full (fulfillment) no permite adjuntar comprobante en MLC"}
    order = json.loads(venta.get("order_json") or "{}")
    pack_id = venta.get("pack_id") or order.get("pack_id") or order.get("id") or oid
    pdf = obtener_pdf_dte_odoo(move_id)
    resp = subir_comprobante_ml(str(pack_id), pdf, oid)
    logger.info(f"[{oid}] Comprobante subido a ML (pack {pack_id}, {len(pdf)} bytes): {resp}")
    return resp


def ejecutar_post_emision(move_id: int, fuente: str, oid: str):
    """Corre las acciones post-emision ACTIVADAS para el canal (pagar / email / adjuntar_ml).
    Nunca propaga excepciones: si algo falla, la venta ya quedo emitida igual."""
    if not move_id:
        return
    cfg = POST_EMIT.get((fuente or "mercadolibre").strip().lower(), {})
    if cfg.get("pagar") == "on":
        try:
            registrar_pago_odoo(move_id)
        except Exception as e:
            logger.error(f"[{oid}] Post-emision 'pagar' fallo: {e}", exc_info=True)
    if cfg.get("email") == "on":
        try:
            enviar_comprobante_email(move_id)
        except Exception as e:
            logger.error(f"[{oid}] Post-emision 'email' fallo: {e}", exc_info=True)
    if cfg.get("adjuntar_ml") == "on":
        try:
            adjuntar_comprobante_ml(oid, move_id)
        except Exception as e:
            logger.error(f"[{oid}] Post-emision 'adjuntar_ml' fallo: {e}", exc_info=True)
    if cfg.get("adjuntar_fl") == "on":
        try:
            adjuntar_comprobante_fl(oid, move_id)
        except Exception as e:
            logger.error(f"[{oid}] Post-emision 'adjuntar_fl' fallo: {e}", exc_info=True)


class SplitParentError(Exception):
    """La venta ML es el pack ORIGINAL de una division (envio 'pack_splitted'): NO se factura.
    Cada orden hija se factura por separado (1 unidad c/u) y sube su PDF a su propio pack."""
    pass


def ml_es_split_parent(venta: dict) -> bool:
    """True si la venta ML es el pack original que fue DIVIDIDO en paquetes (envio en substatus
    'pack_splitted'). Solo se consulta para ventas ML con pack_id. Defensivo: ante error -> False."""
    if (venta.get("fuente") or "mercadolibre") != "mercadolibre":
        return False
    if not venta.get("pack_id"):
        return False
    try:
        order = json.loads(venta.get("order_json") or "{}")
        sid = (order.get("shipping") or {}).get("id")
        if not sid:
            return False
        # Llamada ACOTADA (8s, sin reintentos ni lock global): esta en el camino caliente de la
        # emision; no debe estancar el worker si ML esta lento/rate-limiteado -> ante duda, False.
        r = requests.get(f"https://api.mercadolibre.com/shipments/{sid}",
                         headers={**ml_headers(), "x-format-new": "true"}, timeout=8)
        if r.status_code != 200:
            return False
        return str((r.json() or {}).get("substatus") or "").lower() == "pack_splitted"
    except Exception as e:
        logger.warning(f"[{venta.get('id')}] no se pudo verificar split (se ignora): {e}")
        return False


def _marcar_venta_dividida(venta: dict):
    """Marca la venta como 'dividida' (no se factura la orden original). Si ya tenia documento
    emitido, crea la NC total (la orden original quedo cancelada por la division)."""
    oid = venta["id"]
    if venta.get("move_id") and venta.get("estado") == "enviado":
        try:
            _crear_nota_credito(venta, "Anulacion automatica: venta dividida en Mercado Libre")
        except Exception as e:
            logger.error(f"[{oid}] No se pudo NC la venta dividida: {e}", exc_info=True)
    update_venta(oid, estado="dividida",
                 error="Venta dividida en ML: se facturan las ordenes hijas por separado")
    logger.info(f"[{oid}] Marcada como DIVIDIDA (pack original, no se factura)")


def emitir_venta(oid: str) -> tuple:
    """Crea el documento en Odoo para una venta y la marca como 'enviado'.
    Lanza excepcion si falla. Idempotente: si ya esta enviada devuelve su move_id."""
    venta = get_venta(oid)
    if not venta:
        raise Exception(f"Venta {oid} no encontrada")
    if venta.get("move_id") and venta.get("estado") == "enviado":
        return venta["move_id"], venta.get("partner_id")
    # ML dividida: no se factura el pack original (se facturan las hijas por separado)
    if ml_es_split_parent(venta):
        _marcar_venta_dividida(venta)
        raise SplitParentError(f"Venta {oid} dividida en ML: no se factura la orden original")
    order = json.loads(venta["order_json"])
    billing = json.loads(venta["billing_json"] or "{}")
    move_id, partner_id = create_document(
        order=order, billing_raw=billing,
        tipo=venta.get("tipo_sugerido") or "Boleta",
        email=venta.get("email") or "",  # create_document aplica default solo si NO es Falabella
        giro=venta.get("giro") or "",
        cliente_override=venta.get("cliente"),
        rut_override=venta.get("rut"),
        direccion_override=venta.get("direccion"),
        ciudad_override=venta.get("ciudad"),
        region_override=venta.get("region"),
    )
    update_venta(oid, estado="enviado", move_id=move_id,
                 partner_id=partner_id if partner_id else None,
                 error=None, enviado_en=datetime.now())
    ejecutar_post_emision(move_id, venta.get("fuente") or "mercadolibre", oid)
    return move_id, partner_id


def es_error_caf(msg: str) -> bool:
    """Heuristica ESTRICTA: solo es 'sin folios CAF' si el error menciona caf/folio JUNTO a una
    palabra de agotamiento. Antes bastaba 'folio'/'caf' en cualquier parte, lo que producia falsos
    positivos que DETENIAN toda la auto-emision por un error no relacionado."""
    m = (msg or "").lower()
    tiene_folio = ("caf" in m) or ("folio" in m)
    if not tiene_folio:
        return False
    agot = ["no ", "sin ", "agot", "disponible", "available", "insufficient", "insuficient",
            "expired", "vencid", "quedan", "remaining", "exhaust", "run out", "no hay"]
    return any(p in m for p in agot)


def marcar_caf_agotado(tipo_caf: str, msg: str = ""):
    """Marca el tipo (boleta/factura) como sin folios y detiene su auto-emision. Persiste."""
    if CAF_STATUS.get(tipo_caf) != "agotado":
        CAF_STATUS[tipo_caf] = "agotado"
        try:
            set_config(f"caf_agotado_{tipo_caf}", "agotado")
        except Exception:
            pass
        logger.error(f"SIN FOLIOS CAF de {tipo_caf.upper()}: auto-emision de {tipo_caf} DETENIDA hasta cargar mas CAF. {str(msg)[:200]}")


def manejar_error_emision(oid: str, tipo_doc: str, e: Exception):
    """Decide el estado de la venta segun el error de emision.
    Si es por falta de folios CAF: detiene la auto-emision de ese tipo y deja la venta PENDIENTE
    (no perdida) con una nota clara. Cualquier otro error: estado 'error'."""
    msg = str(e)
    tipo_caf = "factura" if tipo_doc == "Factura" else "boleta"
    if es_error_caf(msg):
        marcar_caf_agotado(tipo_caf, msg)
        update_venta(oid, estado="pendiente", error=f"Sin folios CAF de {tipo_caf}: solicitar mas CAF")
    else:
        update_venta(oid, estado="error", error=msg[:500])


def auto_emitir_venta(oid: str):
    """Emision automatica al ingresar la compra, segun el interruptor:
      Boleta  -> se emite si AUTO_EMIT_BOLETAS == 'auto'
      Factura -> se emite si AUTO_EMIT_FACTURAS == 'auto' Y tiene datos completos
    En cualquier otro caso la venta queda 'pendiente' para autorizar a mano.
    Nunca propaga la excepcion (no debe tumbar el worker de webhooks)."""
    venta = get_venta(oid)
    if not venta or venta.get("estado") != "pendiente":
        return
    fuente = (venta.get("fuente") or "mercadolibre").strip().lower()
    cfg = AUTO_EMIT.get(fuente, AUTO_EMIT["mercadolibre"])
    tipo = venta.get("tipo_sugerido") or "Boleta"
    if tipo == "Factura":
        if cfg.get("facturas") != "auto":
            logger.info(f"[{oid}] Factura ({fuente}) en modo manual, queda pendiente")
            return
        if not datos_completos_para_factura(venta):
            logger.info(f"[{oid}] Auto-emision pospuesta: factura con datos incompletos, queda pendiente")
            return
    else:
        if cfg.get("boletas") != "auto":
            logger.info(f"[{oid}] Boleta ({fuente}) en modo manual, queda pendiente")
            return
    tipo_caf = "factura" if tipo == "Factura" else "boleta"
    if CAF_STATUS.get(tipo_caf) == "agotado":
        logger.warning(f"[{oid}] CAF de {tipo_caf} agotado: auto-emision detenida, queda pendiente")
        return
    try:
        move_id, _ = emitir_venta(oid)
        logger.info(f"[{oid}] Auto-emitida: {tipo} move_id={move_id}")
    except SplitParentError as e:
        logger.info(f"[{oid}] {e} (quedo marcada 'dividida', no es error)")
    except Exception as e:
        logger.error(f"[{oid}] Error en auto-emision: {e}", exc_info=True)
        manejar_error_emision(oid, tipo, e)


def _crear_nota_credito(venta: dict, motivo: str) -> Optional[int]:
    """Crea y publica la NC en Odoo para la venta dada. Marca la venta como 'nota_credito'.
    Lanza excepcion si falla. Reutilizada por el endpoint manual y el automatismo de cancelacion."""
    oid = venta["id"]
    move_id = venta.get("move_id")
    if not move_id:
        raise Exception("La venta no tiene documento en Odoo")
    ctx = odoo_connect()
    moves = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": ["state", "name"]})
    if not moves:
        raise Exception(f"Documento {move_id} no encontrado en Odoo")
    move = moves[0]
    if move["state"] != "posted":
        raise Exception(f"Documento {move['name']} no esta publicado (estado: {move['state']})")
    from datetime import date
    # Odoo 17-18 usa refund_method, Odoo 19 renombro a refund_type
    reversal_fields = odoo_exec(ctx, "account.move.reversal", "fields_get", [], {"attributes": ["string"]})
    reversal_vals = {
        "move_ids": [(6, 0, [move_id])],
        "date": date.today().isoformat(),
        "reason": motivo,
        "journal_id": False,
    }
    if "refund_method" in reversal_fields:
        reversal_vals["refund_method"] = "cancel"
    elif "refund_type" in reversal_fields:
        reversal_vals["refund_type"] = "cancel"
    wizard_id = odoo_exec(ctx, "account.move.reversal", "create", [reversal_vals])
    result = odoo_exec(ctx, "account.move.reversal", "reverse_moves", [[wizard_id]])
    nc_move_id = None
    if isinstance(result, dict):
        domain = result.get("domain")
        if domain:
            ncs = odoo_exec(ctx, "account.move", "search", [domain])
            if ncs:
                nc_move_id = ncs[0]
        res_id = result.get("res_id")
        if res_id:
            nc_move_id = res_id
    if not nc_move_id:
        ncs = odoo_exec(ctx, "account.move", "search", [[["reversed_entry_id", "=", move_id]]], {"limit": 1})
        if ncs:
            nc_move_id = ncs[0]
    if nc_move_id:
        nc_state = odoo_exec(ctx, "account.move", "read", [[nc_move_id]], {"fields": ["state"]})[0]["state"]
        if nc_state == "draft":
            odoo_exec(ctx, "account.move", "action_post", [[nc_move_id]])
        logger.info(f"[{oid}] NC creada: move_id={nc_move_id}")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE ventas SET estado='nota_credito', nc_motivo=%s WHERE id=%s", (motivo, oid))
        conn.commit()
    return nc_move_id


def _nc_id_desde_result(ctx, result, move_id: int) -> Optional[int]:
    """Extrae el id de la NC recien creada por el wizard de reversion."""
    nc_id = None
    if isinstance(result, dict):
        domain = result.get("domain")
        if domain:
            ncs = odoo_exec(ctx, "account.move", "search", [domain])
            if ncs:
                nc_id = ncs[0]
        if result.get("res_id"):
            nc_id = result["res_id"]
    if not nc_id:
        ncs = odoo_exec(ctx, "account.move", "search",
                        [[["reversed_entry_id", "=", move_id]]], {"limit": 1, "order": "id desc"})
        if ncs:
            nc_id = ncs[0]
    return nc_id


def _crear_nota_credito_parcial(venta: dict, creditos: list, motivo: str) -> int:
    """Crea una NC PARCIAL: acredita solo los items/cantidades indicados en `creditos`
    (lista de {line_index, cantidad}). La factura original QUEDA VIGENTE (no se anula);
    la venta permanece 'enviado'. Usa el wizard con refund_method='refund' para generar la
    NC en borrador (tipo doc + diario + referencia correctos de la localizacion CL), luego
    recorta las lineas a lo devuelto y la publica."""
    oid = venta["id"]
    move_id = venta.get("move_id")
    if not move_id:
        raise Exception("La venta no tiene documento en Odoo")
    if not creditos:
        raise Exception("No se indicaron items a acreditar")
    ctx = odoo_connect()
    mv = odoo_exec(ctx, "account.move", "read", [[move_id]], {"fields": ["state", "name"]})
    if not mv:
        raise Exception(f"Documento {move_id} no encontrado en Odoo")
    if mv[0]["state"] != "posted":
        raise Exception(f"Documento {mv[0]['name']} no esta publicado (estado: {mv[0]['state']})")

    # Cantidad total del DTE original (para el guard de mapeo por indice)
    orig_prod = obtener_lineas_dte_odoo(move_id)
    cred_by_idx = {int(c["line_index"]): float(c["cantidad"]) for c in creditos if float(c.get("cantidad") or 0) > 0}
    if not cred_by_idx:
        raise Exception("No se indicaron cantidades a acreditar")

    from datetime import date
    reversal_fields = odoo_exec(ctx, "account.move.reversal", "fields_get", [], {"attributes": ["string"]})
    reversal_vals = {
        "move_ids": [(6, 0, [move_id])],
        "date": date.today().isoformat(),
        "reason": motivo,
        "journal_id": False,
    }
    # 'refund' crea la NC en BORRADOR (copia total) para poder recortarla; 'cancel' reversa todo.
    if "refund_method" in reversal_fields:
        reversal_vals["refund_method"] = "refund"
    elif "refund_type" in reversal_fields:
        reversal_vals["refund_type"] = "refund"
    wizard_id = odoo_exec(ctx, "account.move.reversal", "create", [reversal_vals])
    result = odoo_exec(ctx, "account.move.reversal", "reverse_moves", [[wizard_id]])
    nc_id = _nc_id_desde_result(ctx, result, move_id)
    if not nc_id:
        raise Exception("No se pudo generar la NC en borrador")

    # Lineas de producto de la NC (mismo orden que el DTE original)
    nc_line_ids = odoo_exec(ctx, "account.move", "read", [[nc_id]], {"fields": ["invoice_line_ids"]})[0].get("invoice_line_ids") or []
    nclines = odoo_exec(ctx, "account.move.line", "read", [nc_line_ids],
                        {"fields": ["name", "quantity", "display_type"]})
    prod_lines = [l for l in nclines if l.get("display_type") in (False, None, "product")]

    if len(prod_lines) != len(orig_prod):
        # El mapeo por indice no es fiable: abortar sin dejar basura.
        odoo_exec(ctx, "account.move", "unlink", [[nc_id]])
        raise Exception(f"No coinciden las lineas de la NC ({len(prod_lines)}) con el DTE ({len(orig_prod)}); NC parcial cancelada")

    commands = []
    for i, l in enumerate(prod_lines):
        q = cred_by_idx.get(i, 0)
        orig_q = l.get("quantity") or 0
        if q <= 0:
            commands.append((2, l["id"]))               # quitar linea no devuelta
        elif q < orig_q:
            commands.append((1, l["id"], {"quantity": q}))  # acreditar cantidad parcial
        # q >= orig_q -> se deja completa
    if commands:
        odoo_exec(ctx, "account.move", "write", [[nc_id], {"invoice_line_ids": commands}])

    # Verificar que quede al menos una linea con monto
    quedan = odoo_exec(ctx, "account.move", "read", [[nc_id]], {"fields": ["invoice_line_ids"]})[0].get("invoice_line_ids") or []
    quedan_prod = [l for l in odoo_exec(ctx, "account.move.line", "read", [quedan], {"fields": ["display_type"]})
                   if l.get("display_type") in (False, None, "product")]
    if not quedan_prod:
        odoo_exec(ctx, "account.move", "unlink", [[nc_id]])
        raise Exception("La NC parcial quedo sin lineas (nada que acreditar)")

    st = odoo_exec(ctx, "account.move", "read", [[nc_id]], {"fields": ["state"]})[0]["state"]
    if st == "draft":
        odoo_exec(ctx, "account.move", "action_post", [[nc_id]])
    logger.info(f"[{oid}] NC PARCIAL creada: move_id={nc_id} (factura {move_id} sigue vigente)")
    # La venta NO cambia a 'nota_credito' (es parcial, la factura sigue viva).
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE ventas SET nc_motivo=%s WHERE id=%s", (f"NC parcial: {motivo}", oid))
        conn.commit()
    return nc_id


def auto_nota_credito_si_cancelado(venta: dict, ml_status: str):
    """Si una orden ML pasa a 'cancelled' y ya tenia documento emitido, crea la NC sola.
    Solo aplica a Mercado Libre. Respeta el interruptor NC_AUTO[ml][total]. Nunca propaga la excepcion."""
    if ml_status != "cancelled":
        return
    if NC_AUTO.get("mercadolibre", {}).get("total") != "on":
        logger.info(f"[{(venta or {}).get('id')}] NC total automatica ML desactivada; queda para NC manual")
        return
    if not venta or venta.get("move_id") is None:
        return
    if venta.get("estado") != "enviado":
        return
    try:
        nc_id = _crear_nota_credito(venta, "Anulacion automatica (cancelada en Mercado Libre)")
        logger.info(f"[{venta.get('id')}] NC automatica por cancelacion ML: nc_move_id={nc_id}")
    except Exception as e:
        logger.error(f"[{venta.get('id')}] Error en NC automatica: {e}", exc_info=True)


# =========================
# PROCESAMIENTO WEBHOOK ML
# =========================

ML_ESTADO_ENVIO = {
    "payment_required": "Pendiente de pago",
    "payment_in_process": "Pago en proceso",
    "paid": "Pagado",
    "ready_to_ship": "Listo para envio",
    "shipped": "En camino",
    "delivered": "Entregado",
    "cancelled": "Cancelado",
    "invalid": "Invalido",
}

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
        if pack_id:
            existing_pack = get_venta_by_pack(pack_id)
            if existing_pack:
                update_venta(existing_pack["id"], estado_envio=estado_envio)
                logger.info(f"[pack:{pack_id}] Estado envio actualizado: {estado_envio}")
                auto_nota_credito_si_cancelado(get_venta(existing_pack["id"]), ml_status)
                return
            if ml_status not in ML_ESTADOS_VALIDOS:
                logger.info(f"[pack:{pack_id}] Estado no valido ({ml_status}), ignorado")
                return
            pack_data = get_ml_pack(pack_id)
            pack_order_ids = [str(o["id"]) for o in (pack_data.get("orders") or [])]
            if not pack_order_ids:
                pack_order_ids = [order_id]
            all_orders = []
            for oid_pack in pack_order_ids:
                o = get_ml_order(oid_pack) if oid_pack != order_id else order
                if o:
                    all_orders.append(o)
            all_items = merge_order_items(all_orders)
            billing_raw = get_ml_billing_raw_safe(order_id)
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
                      direccion, email, ciudad, region,
                      pack_id=pack_id, order_items_override=all_items)
            logger.info(f"[pack:{pack_id}] Pack consolidado: {len(all_orders)} ordenes, {len(all_items)} items")
            auto_emitir_venta(pack_id)
            return
        existing = get_venta(order_id)
        if existing:
            update_venta(order_id, estado_envio=estado_envio)
            logger.info(f"[{order_id}] Estado envio actualizado: {estado_envio}")
            auto_nota_credito_si_cancelado(get_venta(order_id), ml_status)
            return
        if ml_status not in ML_ESTADOS_VALIDOS:
            logger.info(f"[{order_id}] Estado no valido ({ml_status}), ignorado")
            return
        billing_raw = get_ml_billing_raw_safe(order_id)
        billing_info = get_billing_info(billing_raw)
        rut = extract_rut(billing_info)
        cliente = extract_name(billing_info, order)
        giro = extract_activity(billing_info, order)
        direccion = extract_direccion(order, billing_info, billing_raw)
        ciudad = extract_ciudad_from_billing(billing_info)
        region = extract_region_from_billing(billing_info)
        email = extract_email(billing_info, order)
        tipo_sugerido = detect_tipo(order, billing_info)
        if not rut or not cliente or cliente == "Cliente ML" or not direccion:
            rut, cliente, direccion, ciudad, region = enrich_from_shipment(
                order, rut, cliente, direccion, ciudad, region
            )

        if tipo_sugerido == "Boleta" and not giro:
            giro = DEFAULT_BOLETA_ACTIVITY
        save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion, email, ciudad, region)
        logger.info(f"[{order_id}] Guardada: {tipo_sugerido} estado={estado_envio}")
        auto_emitir_venta(order_id)
    except Exception as e:
        logger.error(f"[{order_id}] Error procesando webhook: {e}", exc_info=True)


def process_webhook_shipment(shipment_id: str):
    """Webhook ML topic 'shipments': persiste el estado REAL del envio
    (status/substatus del shipment) en columnas aparte, SIN tocar estado_envio
    ni la emision de boletas. PUSH, no barrido: 1 fetch del shipment (throttleado
    por _ml_lock) por evento; resuelve la venta por order_id directo o por pack.
    Asi la app de Lemulux ve 'shipped/delivered' sin que nadie consulte ML en masa."""
    try:
        shipment = get_ml_shipment(shipment_id)
        if not shipment:
            logger.warning(f"[ship:{shipment_id}] Shipment no encontrado en ML")
            return
        status = shipment.get("status") or ""
        substatus = shipment.get("substatus") or ""
        order_id = str(shipment.get("order_id") or "")
        if not order_id or not status:
            return
        venta = get_venta(order_id)              # venta de orden simple (id=order_id)
        if not venta:
            order = get_ml_order(order_id)        # solo si no esta directa: resolver el pack
            pack_id = str((order or {}).get("pack_id") or "")
            if pack_id:
                venta = get_venta_by_pack(pack_id)
        if not venta:
            logger.info(f"[ship:{shipment_id}] Sin venta local para order {order_id} (ignorado)")
            return
        if venta.get("estado_envio_real") == status and venta.get("estado_envio_sub") == substatus:
            return  # sin cambios -> no escribir
        update_venta(venta["id"], estado_envio_real=status, estado_envio_sub=substatus)
        logger.info(f"[ship:{shipment_id}] envio real={status}/{substatus} -> venta {venta['id']}")
    except Exception as e:
        logger.error(f"[ship:{shipment_id}] Error procesando shipment: {e}", exc_info=True)


def reprocesar_venta_desde_ml(order_id: str):
    venta = get_venta(order_id)
    order = get_ml_order(order_id)
    first_real_order_id = order_id
    if not order:
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
    billing_raw = get_ml_billing_raw_safe(first_real_order_id)
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
    if not rut or not cliente or cliente == "Cliente ML" or not direccion:
        rut, cliente, direccion, ciudad, region = enrich_from_shipment(
            order, rut, cliente, direccion, ciudad, region
        )

    save_venta(order, billing_raw, tipo_sugerido, cliente, rut, giro, direccion, email, ciudad, region,
               forzar_actualizacion=True)
    items, item_count, total_bruto = summarize_order_items(order)
    return {
        "id": str(order_id), "cliente": cliente, "rut": rut, "email": email,
        "giro": giro, "direccion": direccion, "tipo_sugerido": tipo_sugerido,
        "items": items, "item_count": item_count, "total_bruto": total_bruto,
    }


# =========================
# COLA WEBHOOKS ML
# =========================

webhook_queue = queue_module.Queue()

# Rastreo simple de webhooks ML recibidos (para /ml/webhook-status). Se reinicia al reiniciar app.
_ml_wh_last = None
_ml_wh_count = 0
_ml_wh_last_topic = None


def webhook_worker():
    global _consecutive_429
    while True:
        try:
            item = webhook_queue.get(timeout=5)
            try:
                if isinstance(item, str) and item.startswith("ship:"):
                    process_webhook_shipment(item[5:])
                else:
                    process_webhook_order(item)
                base_delay = 3
                extra = min(_consecutive_429 * 2, 30)
                time.sleep(base_delay + extra)
            except Exception as e:
                logger.error(f"[{item}] Error en webhook worker: {e}")
                if "demasiados errores" in str(e) or "429" in str(e):
                    time.sleep(30)
                else:
                    time.sleep(3)
            finally:
                webhook_queue.task_done()
        except queue_module.Empty:
            continue


def get_ml_seller_id() -> Optional[str]:
    try:
        data = ml_get("https://api.mercadolibre.com/users/me")
        return str(data.get("id", ""))
    except Exception as e:
        logger.error(f"No se pudo obtener seller_id: {e}")
        return None


def get_ml_ordenes_recientes(seller_id: str, total: int = 200) -> list:
    estados = ["paid"]
    todas = []
    por_estado = total // len(estados) + 50
    for estado in estados:
        offset = 0
        limit = 50
        while len([o for o in todas if o.get("status") == estado]) < por_estado:
            url = (f"https://api.mercadolibre.com/orders/search"
                   f"?seller={seller_id}&order.status={estado}&sort=date_desc"
                   f"&limit={limit}&offset={offset}")
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
            time.sleep(2)
    visto = set()
    unicas = []
    for o in todas:
        oid = str(o.get("id", ""))
        if oid and oid not in visto:
            visto.add(oid)
            unicas.append(o)
    unicas.sort(key=lambda o: o.get("date_created", ""), reverse=True)
    return unicas[:total]


def get_ml_ordenes_canceladas(seller_id: str, total: int = 100) -> list:
    """Ordenes ML recientes en estado 'cancelled' (incluye las que se cancelaron al DIVIDIR una
    venta). El buscador normal solo trae 'paid', por eso las canceladas se consultan aparte."""
    todas = []
    offset = 0
    limit = 50
    while len(todas) < total:
        url = (f"https://api.mercadolibre.com/orders/search"
               f"?seller={seller_id}&order.status=cancelled&sort=date_desc"
               f"&limit={limit}&offset={offset}")
        try:
            data = ml_get(url)
        except Exception as e:
            logger.warning(f"Error paginando canceladas ML offset={offset}: {e}")
            break
        res = data.get("results") or []
        if not res:
            break
        todas.extend(res)
        offset += limit
        if len(res) < limit:
            break
        time.sleep(2)
    return todas[:total]


def revisar_canceladas_ml(seller_id: str) -> int:
    """Para las ordenes ML canceladas recientes que YA tenemos emitidas, dispara la NC total
    (respeta NC_AUTO[ml][total]). Cubre el caso de una venta que se cancela/divide DESPUES de
    haberse facturado (el reconciliador normal no la ve porque solo mira 'paid'). Idempotente."""
    nc = 0
    for o in get_ml_ordenes_canceladas(seller_id, total=100):
        try:
            oid = str(o.get("id") or "")
            pack_id = str(o.get("pack_id") or "")
            v = (get_venta_by_pack(pack_id) if pack_id else None) or get_venta(oid)
            if v and v.get("estado") == "enviado" and v.get("move_id"):
                auto_nota_credito_si_cancelado(v, "cancelled")
                nc += 1
        except Exception as e:
            logger.error(f"[ML] revisar canceladas: {e}")
        time.sleep(0.3)
    return nc


def reconciliar_ordenes_ml():
    while True:
        # Si hay 429 recientes, esperar mas antes de reconciliar
        base_wait = 15 * 60
        extra = min(_consecutive_429 * 60, 10 * 60)
        time.sleep(base_wait + extra)
        if _consecutive_429 > 3:
            logger.info(f"Reconciliacion ML pospuesta: {_consecutive_429} 429s recientes, esperando recuperacion")
            continue
        try:
            seller_id = get_ml_seller_id()
            if not seller_id:
                continue
            ordenes_ml = get_ml_ordenes_recientes(seller_id, total=200)
            if not ordenes_ml:
                continue
            ids_ml = [str(o["id"]) for o in ordenes_ml]
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_ml,))
                    ids_en_bd = {str(row["id"]) for row in cur.fetchall()}
            faltantes = [oid for oid in ids_ml if oid not in ids_en_bd]
            if faltantes:
                logger.warning(f"Reconciliacion auto ML: {len(faltantes)} faltantes, encolando hasta 10")
                for oid in faltantes[:10]:
                    if oid not in list(webhook_queue.queue):
                        webhook_queue.put(oid)
                    time.sleep(0.5)
            else:
                logger.info(f"Reconciliacion ML OK: {len(ordenes_ml)} ordenes todas en BD")
            # Re-chequear canceladas recientes ya emitidas -> NC (cubre divisiones/cancelaciones
            # posteriores a la facturacion, que el buscador de 'paid' no ve).
            try:
                _nc = revisar_canceladas_ml(seller_id)
                if _nc:
                    logger.info(f"[ML] revisar canceladas: {_nc} ordenes emitidas canceladas evaluadas para NC")
            except Exception as e:
                logger.error(f"[ML] Error revisando canceladas: {e}")
        except Exception as e:
            logger.error(f"Error en reconciliacion ML: {e}")


# =========================
# WOOCOMMERCE
# =========================

WC_DEFAULT_EMAIL = "boleta@lemulux.com"
WC_FIELD_RUT         = "billing_rut"
WC_FIELD_TIPODOC     = "billing_tipodoc"
WC_FIELD_COMPANY     = "billing_company"
WC_FIELD_RUT_EMPRESA = "billing_rut_empresa"
WC_FIELD_GIRO        = "billing_giro"

WC_ESTADOS_VALIDOS = {"processing", "completed"}

WC_STATE_TO_REGION = {
    "AI": "Aysen del Gral. Carlos Ibanez del Campo",
    "AN": "Antofagasta",
    "AP": "Arica y Parinacota",
    "AR": "de la Araucania",
    "AT": "Atacama",
    "BI": "del BioBio",
    "CO": "Coquimbo",
    "LI": "del Libertador Gral. Bernardo O'Higgins",
    "LL": "de los Lagos",
    "LR": "Los Rios",
    "MA": "Magallanes",
    "ML": "del Maule",
    "NB": "del Nuble",
    "RM": "Metropolitana",
    "TA": "Tarapaca",
    "VS": "Valparaiso",
}

wc_webhook_queue = queue_module.Queue()


def wc_webhook_worker():
    while True:
        try:
            order_id = wc_webhook_queue.get(timeout=5)
            try:
                process_wc_order(str(order_id))
                time.sleep(2)
            except Exception as e:
                logger.error(f"[WC:{order_id}] Error en worker: {e}")
                time.sleep(5)
            finally:
                wc_webhook_queue.task_done()
        except queue_module.Empty:
            continue


def wc_get(path: str) -> dict:
    url = get_env("WC_URL", required=False, default="").rstrip("/") + "/wp-json/wc/v3/" + path.lstrip("/")
    key    = get_env("WC_CONSUMER_KEY", required=False, default="")
    secret = get_env("WC_CONSUMER_SECRET", required=False, default="")
    if not key or not secret:
        raise Exception("WC_CONSUMER_KEY o WC_CONSUMER_SECRET no configurados")
    try:
        res = requests.get(url, auth=(key, secret), timeout=30)
        if res.status_code == 404:
            return {}
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logger.error(f"[WC] Error GET {path}: {e}")
        raise


def get_wc_order(order_id: str) -> dict:
    return wc_get(f"orders/{order_id}")


def get_wc_orders_recent(total: int = 100) -> list:
    orders = []
    page = 1
    per_page = 50
    while len(orders) < total:
        try:
            data = wc_get(f"orders?status=processing,completed&per_page={per_page}&page={page}&orderby=date&order=desc")
        except Exception as e:
            logger.warning(f"[WC] Error paginando ordenes pagina={page}: {e}")
            break
        if not data or not isinstance(data, list):
            break
        orders.extend(data)
        if len(data) < per_page:
            break
        page += 1
        time.sleep(1)
    return orders[:total]


def wc_get_meta(order: dict, key: str) -> str:
    for meta in order.get("meta_data") or []:
        if meta.get("key") == key:
            val = meta.get("value")
            if isinstance(val, list):
                return ", ".join(str(v) for v in val if v)
            return str(val).strip() if val else ""
    return ""


def wc_extract_tipodoc(order: dict) -> str:
    # Si lleno giro o RUT empresa -> Factura sin importar billing_tipodoc
    giro = wc_get_meta(order, WC_FIELD_GIRO).strip()
    rut_empresa = wc_get_meta(order, WC_FIELD_RUT_EMPRESA).strip()
    if giro or rut_empresa:
        return "Factura"
    # Fallback: revisar el campo tipodoc explicito
    tipodoc_raw = wc_get_meta(order, WC_FIELD_TIPODOC).strip().lower()
    if "factura" in tipodoc_raw:
        return "Factura"
    return "Boleta"


def wc_extract_rut(order: dict, tipo: str) -> str:
    if tipo == "Factura":
        rut = wc_get_meta(order, WC_FIELD_RUT_EMPRESA).strip()
    else:
        rut = wc_get_meta(order, WC_FIELD_RUT).strip()
    return normalize_rut(rut) if rut else ""


def wc_extract_nombre(order: dict, tipo: str) -> str:
    billing = order.get("billing") or {}
    if tipo == "Factura":
        company = (wc_get_meta(order, WC_FIELD_COMPANY) or billing.get("company") or "").strip()
        if company:
            return company
    first = (billing.get("first_name") or "").strip()
    last  = (billing.get("last_name") or "").strip()
    full  = f"{first} {last}".strip()
    return full or "Cliente WC"


def wc_extract_email(order: dict) -> str:
    billing = order.get("billing") or {}
    return (billing.get("email") or WC_DEFAULT_EMAIL).strip()


def wc_extract_giro(order: dict, tipo: str) -> str:
    if tipo == "Factura":
        return wc_get_meta(order, WC_FIELD_GIRO).strip()
    return DEFAULT_BOLETA_ACTIVITY


def wc_extract_direccion(order: dict) -> str:
    billing = order.get("billing") or {}
    addr1 = (billing.get("address_1") or "").strip()
    addr2 = (billing.get("address_2") or "").strip()
    if addr1 and addr2:
        return f"{addr1}, {addr2}"
    return addr1 or addr2


def wc_extract_ciudad(order: dict) -> str:
    billing = order.get("billing") or {}
    return (billing.get("city") or "").strip()


def wc_extract_region(order: dict) -> str:
    billing = order.get("billing") or {}
    state_code = (billing.get("state") or "").strip().upper()
    return WC_STATE_TO_REGION.get(state_code, state_code)


def wc_build_order_items(order: dict) -> list:
    items = []
    for li in order.get("line_items") or []:
        qty = float(li.get("quantity") or 1) or 1
        # WooCommerce entrega los montos de linea en NETO (sin IVA); el IVA va aparte
        # en total_tax. create_document espera precio BRUTO (con IVA) porque despues
        # divide por 1.19, asi que reconstruimos el bruto = (total + total_tax) / qty.
        total     = float(li.get("total") or 0)
        total_tax = float(li.get("total_tax") or 0)
        bruto_linea = total + total_tax
        if bruto_linea > 0:
            unit_price = bruto_linea / qty
        else:
            # Fallback: 'price' es el neto por unidad -> pasarlo a bruto
            unit_price = float(li.get("price") or 0) * IVA_RATE
        items.append({
            "item":       {"title": li.get("name") or "Producto WC"},
            "quantity":   qty,
            "unit_price": round(unit_price, 4),
        })
    # Envio: WooCommerce lo entrega en shipping_lines (total neto + total_tax aparte).
    # Se agrega como una linea mas, tambien en BRUTO, para que el total del DTE coincida.
    for sl in order.get("shipping_lines") or []:
        s_total     = float(sl.get("total") or 0)
        s_total_tax = float(sl.get("total_tax") or 0)
        s_bruto = s_total + s_total_tax
        if s_bruto > 0:
            items.append({
                "item":       {"title": sl.get("method_title") or "Despacho"},
                "quantity":   1,
                "unit_price": round(s_bruto, 4),
            })
    return items


def wc_build_fake_order(wc_order: dict) -> dict:
    return {
        "id":          wc_order.get("id"),
        "status":      wc_order.get("status"),
        "order_items": wc_build_order_items(wc_order),
        "buyer": {
            "email":      wc_extract_email(wc_order),
            "first_name": (wc_order.get("billing") or {}).get("first_name", ""),
            "last_name":  (wc_order.get("billing") or {}).get("last_name", ""),
        },
    }


def process_wc_order(order_id: str):
    try:
        wc_order = get_wc_order(order_id)
        if not wc_order:
            logger.warning(f"[WC:{order_id}] Orden no encontrada")
            return
        wc_status = wc_order.get("status", "")
        oid_str = f"WC-{order_id}"
        existing = get_venta(oid_str)
        if existing:
            update_venta(oid_str, estado_envio=wc_status)
            logger.info(f"[WC:{order_id}] Estado actualizado: {wc_status}")
            return
        if wc_status not in WC_ESTADOS_VALIDOS:
            logger.info(f"[WC:{order_id}] Estado no valido ({wc_status}), ignorado")
            return
        tipo      = wc_extract_tipodoc(wc_order)
        rut       = wc_extract_rut(wc_order, tipo)
        nombre    = wc_extract_nombre(wc_order, tipo)
        email     = wc_extract_email(wc_order)
        giro      = wc_extract_giro(wc_order, tipo)
        direccion = wc_extract_direccion(wc_order)
        ciudad    = wc_extract_ciudad(wc_order)
        region    = wc_extract_region(wc_order)
        fake_order   = wc_build_fake_order(wc_order)
        billing_fake = {}
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = %s", (oid_str,))
                if cur.fetchone():
                    return
                cur.execute(
                    """
                    INSERT INTO ventas
                        (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region,
                         tipo_sugerido, estado, estado_envio, order_json, billing_json, fuente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s, 'woocommerce')
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        oid_str, None,
                        (nombre or "Cliente WC").strip(),
                        normalize_rut(rut) if rut else "",
                        (email or WC_DEFAULT_EMAIL).strip(),
                        (giro or "").strip(),
                        (direccion or "").strip(),
                        (ciudad or "").strip(),
                        (region or "").strip(),
                        tipo, wc_status,
                        json.dumps(fake_order, ensure_ascii=False),
                        json.dumps(billing_fake, ensure_ascii=False),
                    ),
                )
            conn.commit()
        logger.info(f"[WC:{order_id}] Guardada -> tipo={tipo} rut={rut or 'sin RUT'} cliente={nombre}")
        auto_emitir_venta(oid_str)
    except Exception as e:
        logger.error(f"[WC:{order_id}] Error procesando: {e}", exc_info=True)


def reprocesar_wc_venta(oid: str) -> dict:
    """Recalcula una venta WooCommerce existente re-consultando la API de WC y
    reconstruyendo el order_json con los montos corregidos (IVA + envio).
    Solo actua sobre ventas NO enviadas (pendiente/error). Devuelve un resumen."""
    venta = get_venta(oid)
    if not venta:
        raise Exception(f"Venta {oid} no encontrada")
    if (venta.get("fuente") or "") != "woocommerce":
        raise Exception(f"La venta {oid} no es de WooCommerce")
    if venta.get("estado") == "enviado":
        raise Exception(f"La venta {oid} ya fue emitida en Odoo; anulala antes de recalcular")
    if venta.get("estado") not in ("pendiente", "error"):
        raise Exception(f"La venta {oid} esta en estado '{venta.get('estado')}', no se recalcula")

    order_id = oid[3:] if oid.startswith("WC-") else oid
    wc_order = get_wc_order(order_id)
    if not wc_order:
        raise Exception(f"Orden WC {order_id} no encontrada en WooCommerce")

    tipo      = wc_extract_tipodoc(wc_order)
    rut       = wc_extract_rut(wc_order, tipo)
    nombre    = wc_extract_nombre(wc_order, tipo)
    email     = wc_extract_email(wc_order)
    giro      = wc_extract_giro(wc_order, tipo)
    direccion = wc_extract_direccion(wc_order)
    ciudad    = wc_extract_ciudad(wc_order)
    region    = wc_extract_region(wc_order)
    fake_order = wc_build_fake_order(wc_order)  # ya con precios brutos corregidos + envio

    update_venta(
        oid,
        estado="pendiente",
        cliente=(nombre or "Cliente WC").strip(),
        rut=normalize_rut(rut) if rut else "",
        email=(email or WC_DEFAULT_EMAIL).strip(),
        giro=(giro or "").strip(),
        direccion=(direccion or "").strip(),
        ciudad=(ciudad or "").strip(),
        region=(region or "").strip(),
        tipo_sugerido=tipo,
        estado_envio=wc_order.get("status", ""),
        order_json=json.dumps(fake_order, ensure_ascii=False),
        error=None,
    )
    _, item_count, total_bruto = summarize_order_items(fake_order)
    logger.info(f"[{oid}] Recalculada desde WC -> tipo={tipo} total_bruto={total_bruto} items={item_count}")
    return {"id": oid, "cliente": nombre, "tipo": tipo, "total_bruto": total_bruto, "item_count": item_count}


def reconciliar_wc_ordenes():
    while True:
        time.sleep(20 * 60)
        try:
            ordenes = get_wc_orders_recent(total=100)
            if not ordenes:
                continue
            ids_wc = [f"WC-{o['id']}" for o in ordenes if o.get("id")]
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_wc,))
                    ids_en_bd = {str(row["id"]) for row in cur.fetchall()}
            faltantes = [o for o in ordenes if f"WC-{o['id']}" not in ids_en_bd]
            if faltantes:
                logger.warning(f"[WC] Reconciliacion: {len(faltantes)} faltantes, encolando hasta 10")
                for o in faltantes[:10]:
                    wc_webhook_queue.put(str(o["id"]))
                    time.sleep(0.5)
            else:
                logger.info(f"[WC] Reconciliacion OK: {len(ordenes)} ordenes en BD")
        except Exception as e:
            logger.error(f"[WC] Error en reconciliacion: {e}")


def verify_wc_webhook(body_bytes: bytes, signature: str) -> bool:
    secret = get_env("WC_WEBHOOK_SECRET", required=False, default="")
    if not secret:
        logger.warning("[WC] WC_WEBHOOK_SECRET no configurado, omitiendo verificacion")
        return True
    mac = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    expected = base64.b64encode(mac.digest()).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def on_startup():
    wait_for_db()
    t = threading.Thread(target=schedule_token_refresh, daemon=True)
    t.start()
    logger.info("Renovacion automatica de token ML iniciada (cada 5h)")
    w = threading.Thread(target=webhook_worker, daemon=True)
    w.start()
    logger.info("Worker de cola de webhooks ML iniciado")
    r = threading.Thread(target=reconciliar_ordenes_ml, daemon=True)
    r.start()
    logger.info("Reconciliador de ordenes ML iniciado (cada 15 min)")
    ww = threading.Thread(target=wc_webhook_worker, daemon=True)
    ww.start()
    logger.info("Worker de cola WooCommerce iniciado")
    wr = threading.Thread(target=reconciliar_wc_ordenes, daemon=True)
    wr.start()
    logger.info("Reconciliador WooCommerce iniciado (cada 20 min)")
    fw = threading.Thread(target=fl_webhook_worker, daemon=True)
    fw.start()
    logger.info("Worker de cola Falabella iniciado")
    fr = threading.Thread(target=reconciliar_fl_ordenes, daemon=True)
    fr.start()
    logger.info(f"Reconciliador Falabella iniciado (cada {os.getenv('FL_RECON_INTERVAL_MIN', '15')} min, hasta {os.getenv('FL_RECON_MAX', '50')} por ciclo)")
    # Cargar overrides persistidos del interruptor (los env vars son solo el default inicial).
    # Migracion: si existen las claves globales viejas, se aplican como default a todos los canales...
    old_b = get_config("auto_emit_boletas")
    old_f = get_config("auto_emit_facturas")
    for _fuente in AUTO_EMIT:
        if old_b in ("auto", "manual"):
            AUTO_EMIT[_fuente]["boletas"] = old_b
        if old_f in ("auto", "manual"):
            AUTO_EMIT[_fuente]["facturas"] = old_f
    # ...y luego los valores por canal (nuevos) sobreescriben.
    for _fuente in AUTO_EMIT:
        for _campo in ("boletas", "facturas"):
            _val = get_config(f"auto_emit_{_fuente}_{_campo}")
            if _val in ("auto", "manual"):
                AUTO_EMIT[_fuente][_campo] = _val
    logger.info(f"Auto-emision -> {AUTO_EMIT}")
    for _fuente in POST_EMIT:
        for _accion in list(POST_EMIT[_fuente].keys()):
            _pv = get_config(f"post_emit_{_fuente}_{_accion}")
            if _pv in ("on", "off"):
                POST_EMIT[_fuente][_accion] = _pv
    logger.info(f"Post-emision -> {POST_EMIT}")
    for _fuente in NC_AUTO:
        for _tipo in list(NC_AUTO[_fuente].keys()):
            _nv = get_config(f"nc_auto_{_fuente}_{_tipo}")
            if _nv in ("on", "off"):
                NC_AUTO[_fuente][_tipo] = _nv
    logger.info(f"NC automatica -> {NC_AUTO}")
    for _tc in CAF_STATUS:
        if get_config(f"caf_agotado_{_tc}") == "agotado":
            CAF_STATUS[_tc] = "agotado"
    logger.info(f"CAF status -> {CAF_STATUS}")


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


@app.get("/config/auto-emision")
def config_auto_emision():
    """Muestra el estado actual del interruptor de auto-emision por canal."""
    return {
        **AUTO_EMIT,
        "post_emit": POST_EMIT,
        "nc_auto": NC_AUTO,
        "caf": CAF_STATUS,
        "nota": "Valores: 'auto' o 'manual' por canal/tipo. Editable en vivo desde el dashboard (POST /config/auto-emision).",
    }


class CafReanudarPayload(BaseModel):
    tipo: str


@app.post("/config/caf/reanudar")
def caf_reanudar(payload: CafReanudarPayload):
    """Reanuda la auto-emision de un tipo tras cargar mas folios CAF (boleta / factura / todos)."""
    tipo = (payload.tipo or "").strip().lower()
    tipos = ["boleta", "factura"] if tipo == "todos" else [tipo]
    for t in tipos:
        if t not in CAF_STATUS:
            raise HTTPException(status_code=400, detail="tipo debe ser 'boleta', 'factura' o 'todos'")
    for t in tipos:
        CAF_STATUS[t] = "ok"
        set_config(f"caf_agotado_{t}", "ok")
    logger.info(f"CAF reanudado: {tipos} -> {CAF_STATUS}")
    return {"ok": True, "caf": CAF_STATUS}


class AutoEmisionPayload(BaseModel):
    fuente: str
    boletas: Optional[str] = None
    facturas: Optional[str] = None


@app.post("/config/auto-emision")
def set_auto_emision(payload: AutoEmisionPayload):
    """Cambia el interruptor de auto-emision de un canal en vivo y lo persiste en la BD."""
    fuente = (payload.fuente or "").strip().lower()
    if fuente not in AUTO_EMIT:
        raise HTTPException(status_code=400, detail="fuente debe ser 'mercadolibre', 'woocommerce' o 'falabella'")
    for campo, valor in (("boletas", payload.boletas), ("facturas", payload.facturas)):
        if valor is None:
            continue
        valor = valor.strip().lower()
        if valor not in ("auto", "manual"):
            raise HTTPException(status_code=400, detail=f"{campo} debe ser 'auto' o 'manual'")
        AUTO_EMIT[fuente][campo] = valor
        set_config(f"auto_emit_{fuente}_{campo}", valor)
    logger.info(f"Auto-emision cambiada [{fuente}] -> {AUTO_EMIT[fuente]}")
    return {"ok": True, "fuente": fuente, **AUTO_EMIT[fuente]}


class PostEmisionPayload(BaseModel):
    fuente: str
    pagar: Optional[str] = None
    email: Optional[str] = None
    adjuntar_ml: Optional[str] = None
    adjuntar_fl: Optional[str] = None


@app.post("/config/post-emision")
def set_post_emision(payload: PostEmisionPayload):
    """Activa/desactiva acciones post-emision (pagar / email / adjuntar_ml / adjuntar_fl) de un canal."""
    fuente = (payload.fuente or "").strip().lower()
    if fuente not in POST_EMIT:
        raise HTTPException(status_code=400, detail="fuente debe ser 'mercadolibre', 'woocommerce' o 'falabella'")
    for campo, valor in (("pagar", payload.pagar), ("email", payload.email),
                         ("adjuntar_ml", payload.adjuntar_ml), ("adjuntar_fl", payload.adjuntar_fl)):
        if valor is None or campo not in POST_EMIT[fuente]:
            continue
        valor = valor.strip().lower()
        if valor not in ("on", "off"):
            raise HTTPException(status_code=400, detail=f"{campo} debe ser 'on' o 'off'")
        POST_EMIT[fuente][campo] = valor
        set_config(f"post_emit_{fuente}_{campo}", valor)
    logger.info(f"Post-emision cambiada [{fuente}] -> {POST_EMIT[fuente]}")
    return {"ok": True, "fuente": fuente, **POST_EMIT[fuente]}


class NcAutoPayload(BaseModel):
    fuente: str
    total: Optional[str] = None
    parcial: Optional[str] = None


@app.post("/config/nc-auto")
def set_nc_auto(payload: NcAutoPayload):
    """Activa/desactiva la Nota de Credito automatica (total / parcial) de un canal."""
    fuente = (payload.fuente or "").strip().lower()
    if fuente not in NC_AUTO:
        raise HTTPException(status_code=400, detail="fuente debe ser 'mercadolibre', 'woocommerce' o 'falabella'")
    for campo, valor in (("total", payload.total), ("parcial", payload.parcial)):
        if valor is None:
            continue
        valor = valor.strip().lower()
        if valor not in ("on", "off"):
            raise HTTPException(status_code=400, detail=f"{campo} debe ser 'on' o 'off'")
        NC_AUTO[fuente][campo] = valor
        set_config(f"nc_auto_{fuente}_{campo}", valor)
    logger.info(f"NC automatica cambiada [{fuente}] -> {NC_AUTO[fuente]}")
    return {"ok": True, "fuente": fuente, **NC_AUTO[fuente]}


@app.get("/debug/shipping/{oid}")
def debug_shipping(oid: str):
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


@app.get("/ml/debug-split/{ident}")
def ml_debug_split(ident: str, extra: str = None):
    """Diagnostico de una venta dividida en ML. Acepta un order_id, un pack_id o el id de la
    venta en nuestra BD. Resuelve por BD -> orden -> pack y muestra las ordenes del pack con el
    estado de cada envio (status/substatus/sibling_id). Solo lectura."""
    def shipment_new(sid):
        if not sid:
            return {}
        try:
            return ml_get(f"https://api.mercadolibre.com/shipments/{sid}", {"x-format-new": "true"})
        except Exception as e:
            return {"error": str(e)}
    def order_safe(oid):
        try:
            return get_ml_order(str(oid)) or {}
        except Exception as e:
            return {"error": str(e)}
    def pack_safe(pid):
        try:
            return get_ml_pack(str(pid)) or {}
        except Exception as e:
            return {"error": str(e)}

    debug = {"ident": ident, "resuelto_por": None}

    # 1) Datos que ya tenemos en la BD (order_json trae order_id real + pack_id + shipping)
    venta = get_venta(ident)
    order_json = {}
    if venta and venta.get("order_json"):
        try:
            order_json = json.loads(venta["order_json"])
        except Exception:
            order_json = {}
    debug["en_bd"] = {
        "existe": bool(venta),
        "estado": (venta or {}).get("estado"),
        "move_id": (venta or {}).get("move_id"),
        "pack_id_bd": (venta or {}).get("pack_id"),
        "order_id_en_json": order_json.get("id"),
        "pack_id_en_json": order_json.get("pack_id"),
        "shipping_id_en_json": (order_json.get("shipping") or {}).get("id"),
    }

    # 2) Candidatos de pack_id y order_id a probar contra ML
    cand_pack = [x for x in [order_json.get("pack_id"), (venta or {}).get("pack_id"), ident] if x]
    cand_order = [x for x in [order_json.get("id"), ident] if x]

    # 3) Intentar como ORDEN
    order = {}
    for oid in cand_order:
        o = order_safe(oid)
        if o and not o.get("error") and o.get("id"):
            order = o
            debug["resuelto_por"] = f"order:{oid}"
            break
    if order:
        debug["orden_consultada"] = {
            "order_id": order.get("id"),
            "pack_id": order.get("pack_id"),
            "status": order.get("status"),
            "shipping_id": (order.get("shipping") or {}).get("id"),
            "items": [{"title": safe_get(it, "item", "title", default=""), "qty": it.get("quantity")}
                      for it in (order.get("order_items") or [])],
        }
        if order.get("pack_id"):
            cand_pack.insert(0, order["pack_id"])

    # 4) Intentar como PACK (el numero visible de ML suele ser el pack)
    pack = {}
    pack_id_ok = None
    for pid in cand_pack:
        p = pack_safe(pid)
        if p and not p.get("error") and (p.get("orders") or p.get("id") or p.get("shipment_id")):
            pack = p
            pack_id_ok = pid
            if not debug["resuelto_por"]:
                debug["resuelto_por"] = f"pack:{pid}"
            break

    def explorar_pack(pid):
        """Resumen de un pack: estado, family/trash y sus ordenes con envio."""
        p = pack_safe(pid)
        if not p or p.get("error"):
            return {"pack_id": str(pid), "error": (p or {}).get("error", "no encontrado")}
        ords = []
        for o in (p.get("orders") or []):
            oid_p = str(o.get("id") or "")
            od = order_safe(oid_p)
            sid = (od.get("shipping") or {}).get("id")
            shp = shipment_new(sid)
            v_bd = get_venta(oid_p)
            # Tambien buscar la venta en BD por el pack (asi vemos como la tenemos guardada)
            v_bd_pack = get_venta(str(pid))
            ords.append({
                "order_id": oid_p,
                "status": od.get("status"),
                "shipping_id": sid,
                "shipment_status": shp.get("status"),
                "shipment_substatus": shp.get("substatus"),
                "sibling_id": shp.get("sibling_id"),
                "items_qty": sum(float(it.get("quantity") or 0) for it in (od.get("order_items") or [])),
                "en_bd_por_order": bool(v_bd),
                "en_bd_por_pack": bool(v_bd_pack),
                "estado_bd": (v_bd or v_bd_pack or {}).get("estado"),
                "move_id_bd": (v_bd or v_bd_pack or {}).get("move_id"),
            })
        return {
            "pack_id": str(pid),
            "status": p.get("status"),
            "family_pack_id": p.get("family_pack_id"),
            "trash_pack_id": p.get("trash_pack_id"),
            "shipment_id": p.get("shipment_id"),
            "ordenes": ords,
        }

    # 5) Explorar la FAMILIA: el pack original + family_pack_id + trash + extras (?extra=id1,id2)
    fam_id = pack.get("family_pack_id") if isinstance(pack, dict) else None
    trash_id = pack.get("trash_pack_id") if isinstance(pack, dict) else None
    extras = [e.strip() for e in (extra or "").split(",") if e.strip()]
    ids_familia, vistos = [], set()
    for pid in [pack_id_ok, fam_id, trash_id] + extras:
        if pid and str(pid) not in vistos:
            vistos.add(str(pid))
            ids_familia.append(pid)
    familia = [explorar_pack(pid) for pid in ids_familia]

    return {
        "ok": True,
        "debug": debug,
        "pack_id_resuelto": pack_id_ok,
        "family_pack_id": fam_id,
        "trash_pack_id": trash_id,
        "pack_raw_keys": list(pack.keys()) if isinstance(pack, dict) else None,
        "pack_status": pack.get("status") if isinstance(pack, dict) else None,
        "familia": familia,
        "nota": ("Se explora el pack original + family_pack_id + trash + los ids que pases en ?extra=. "
                 "Si las hijas no aparecen, corre de nuevo agregando ?extra=2000013934540451,2000013934540453"),
    }


@app.get("/debug/direccion/{oid}")
def debug_direccion(oid: str):
    order = get_ml_order(oid)
    billing_raw = get_ml_billing_raw_safe(oid)
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
            key=score_address_candidate, reverse=True
        )[:20]
    }


@app.get("/debug/ml-scopes")
def debug_ml_scopes():
    """Muestra los permisos (scopes) del token de ML. Necesitas 'write' para cargar comprobantes."""
    refresh_ml_token()  # refresca y captura el scope actual
    scope = _ml_scope or ""
    tiene_write = "write" in scope.lower()
    return {
        "scope": scope or "desconocido",
        "tiene_write": tiene_write,
        "interpretacion": ("OK: la app tiene permiso de escritura, puede cargar comprobantes"
                           if tiene_write else
                           "FALTA 'write': la app solo puede leer. Hay que agregar el scope 'write' a la app de ML y reautorizar."),
    }


@app.get("/debug/ml-fiscal/{oid}")
def debug_ml_fiscal(oid: str):
    """Prueba de solo lectura: intenta LEER los comprobantes del pack en ML.
    status 200 => la app accede al recurso; 401/403 => falta permiso/scope."""
    venta = get_venta(oid)
    order = {}
    if venta and venta.get("order_json"):
        try:
            order = json.loads(venta["order_json"])
        except Exception:
            order = {}
    pack_id = (venta or {}).get("pack_id") or order.get("pack_id") or order.get("id") or oid
    url = f"https://api.mercadolibre.com/packs/{pack_id}/fiscal_documents"
    try:
        r = requests.get(url, headers=ml_headers(), timeout=30)
    except Exception as e:
        return {"pack_id": str(pack_id), "error": str(e)}
    try:
        body = r.json()
    except Exception:
        body = r.text[:800]
    return {
        "pack_id": str(pack_id),
        "status_code": r.status_code,
        "puede_acceder": r.status_code == 200,
        "interpretacion": ("OK: la app puede leer/cargar comprobantes en este pack"
                           if r.status_code == 200 else
                           "Sin permiso (falta scope 'write' o la app no tiene acceso): revisar en developers.mercadolibre.cl"
                           if r.status_code in (401, 403) else "Ver respuesta_ml"),
        "respuesta_ml": body,
    }


@app.post("/ml/webhook")
async def ml_webhook(request: Request):
    global _ml_wh_last, _ml_wh_count, _ml_wh_last_topic
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body invalido"})
    topic = body.get("topic", "")
    resource = body.get("resource", "")
    # Registrar que ML nos esta llamando (para el diagnostico /ml/webhook-status)
    _ml_wh_last = datetime.utcnow().isoformat()
    _ml_wh_count += 1
    _ml_wh_last_topic = topic
    if not resource:
        return {"ok": True, "ignored": True}
    res_id = resource.strip("/").split("/")[-1]
    if not res_id:
        return {"ok": True}
    if topic == "orders_v2":
        if res_id not in list(webhook_queue.queue):
            webhook_queue.put(res_id)
            logger.info(f"Webhook ML encolado: {res_id} (cola: {webhook_queue.qsize()})")
        else:
            logger.info(f"Webhook ML duplicado ignorado: {res_id}")
        return {"ok": True, "order_id": res_id, "queued": webhook_queue.qsize()}
    if topic == "shipments":
        # PUSH del estado de envio: encolar el shipment (dedup). Se procesa
        # de a uno en el worker (throttleado). Nunca dispara barridos.
        key = f"ship:{res_id}"
        if key not in list(webhook_queue.queue):
            webhook_queue.put(key)
            logger.info(f"Webhook ML envio encolado: {res_id} (cola: {webhook_queue.qsize()})")
        else:
            logger.info(f"Webhook ML envio duplicado ignorado: {res_id}")
        return {"ok": True, "shipment_id": res_id, "queued": webhook_queue.qsize()}
    return {"ok": True, "ignored": True}


@app.get("/ml/webhook-status")
def ml_webhook_status():
    """Diagnostico del webhook ML: si estamos recibiendo notificaciones y si ML tiene 'missed feeds'
    (avisos que intento entregar y fallaron -> URL mal configurada o app caida al enviarlos)."""
    app_id = os.getenv("ML_CLIENT_ID", "")
    missed = None
    missed_error = None
    try:
        data = ml_get(f"https://api.mercadolibre.com/missed_feeds?app_id={app_id}") if app_id else {}
        # La respuesta suele traer 'messages' o similar; devolvemos crudo + un conteo aproximado.
        if isinstance(data, dict):
            msgs = data.get("messages") or data.get("results") or []
            missed = {"total_aprox": len(msgs) if isinstance(msgs, list) else data.get("total"),
                      "muestra": (msgs[:5] if isinstance(msgs, list) else data)}
        else:
            missed = data
    except Exception as e:
        missed_error = str(e)
    return {
        "ok": True,
        "callback_esperado": "https://lemulux-odoo-production.up.railway.app/ml/webhook",
        "topic_esperado": "orders_v2 + shipments",
        "recibidos_desde_arranque": _ml_wh_count,
        "ultimo_recibido_utc": _ml_wh_last,
        "ultimo_topic": _ml_wh_last_topic,
        "cola_actual": webhook_queue.qsize(),
        "missed_feeds_ml": missed,
        "missed_feeds_error": missed_error,
        "interpretacion": ("Si 'recibidos_desde_arranque' sube al generarse ventas -> el webhook llega. "
                           "Si hay 'missed_feeds' -> ML intenta avisar pero la URL/topic falla. "
                           "Si ambos vacios -> el webhook NO esta configurado en la app de ML."),
    }


@app.post("/wc/webhook")
async def wc_webhook(request: Request):
    body_bytes = await request.body()
    signature  = request.headers.get("X-WC-Webhook-Signature", "")
    topic      = request.headers.get("X-WC-Webhook-Topic", "")

    if not verify_wc_webhook(body_bytes, signature):
        logger.warning(f"[WC] Firma HMAC invalida en webhook topic={topic}")
        return JSONResponse(status_code=401, content={"error": "Firma invalida"})

    if topic not in ("order.created", "order.updated", "order.status_changed"):
        return {"ok": True, "ignored": True, "topic": topic}

    try:
        body = json.loads(body_bytes)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body invalido"})

    order_id = str(body.get("id") or "")
    status   = str(body.get("status") or "")

    if not order_id:
        return {"ok": True}

    if status not in WC_ESTADOS_VALIDOS:
        logger.info(f"[WC:{order_id}] Estado {status} ignorado")
        return {"ok": True, "ignored": True, "status": status}

    if order_id not in list(wc_webhook_queue.queue):
        wc_webhook_queue.put(order_id)
        logger.info(f"[WC] Webhook encolado: order_id={order_id} status={status}")

    return {"ok": True, "order_id": order_id, "status": status, "queued": wc_webhook_queue.qsize()}


@app.post("/wc/reconciliar")
def wc_reconciliar_manual():
    try:
        ordenes = get_wc_orders_recent(total=100)
        ids_wc  = [f"WC-{o['id']}" for o in ordenes if o.get("id")]
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_wc,))
                ids_en_bd = {str(row["id"]) for row in cur.fetchall()}
        faltantes_ids = [str(o["id"]) for o in ordenes if f"WC-{o['id']}" not in ids_en_bd]
        def encolar():
            for oid in faltantes_ids:
                wc_webhook_queue.put(oid)
                time.sleep(1)
            logger.info(f"[WC] Reconciliacion manual: {len(faltantes_ids)} encoladas")
        threading.Thread(target=encolar, daemon=True).start()
        return {
            "ok": True, "total_wc": len(ordenes), "en_bd": len(ids_en_bd),
            "faltantes": len(faltantes_ids), "encoladas": faltantes_ids[:5],
            "mensaje": f"Procesando {len(faltantes_ids)} ordenes WC faltantes en background."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/wc/ingresar/{order_id}")
def wc_ingresar_manual(order_id: str):
    try:
        process_wc_order(order_id)
        venta = get_venta(f"WC-{order_id}")
        if venta:
            return {"ok": True, "id": f"WC-{order_id}", "cliente": venta.get("cliente"), "estado": venta.get("estado")}
        return {"ok": True, "id": f"WC-{order_id}", "message": "Procesado, verificar en dashboard"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/wc/recalcular-pendientes")
def wc_recalcular_pendientes():
    """Recalcula (arregla montos IVA + envio) todas las ventas WooCommerce NO enviadas.
    Corre en background porque hace una consulta a WooCommerce por cada venta."""
    pendientes = [v for v in (list_ventas("pendiente") + list_ventas("error"))
                  if (v.get("fuente") or "") == "woocommerce"]
    ids = [str(v["id"]) for v in pendientes]
    if not ids:
        return {"ok": True, "total": 0, "mensaje": "No hay ventas WC pendientes/con error para recalcular"}
    def procesar():
        ok = 0
        err = 0
        for oid in ids:
            try:
                reprocesar_wc_venta(oid)
                ok += 1
            except Exception as e:
                err += 1
                logger.error(f"[{oid}] Error recalculando WC: {e}")
            time.sleep(1)
        logger.info(f"[WC] Recalculo de pendientes: {ok} OK, {err} errores")
    threading.Thread(target=procesar, daemon=True).start()
    return {"ok": True, "total": len(ids),
            "mensaje": f"Recalculando {len(ids)} ventas WC en background. Revisa el dashboard en unos momentos."}


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="No se recibio code")
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
            set_ml_refresh_token(data["refresh_token"])
    return JSONResponse(status_code=res.status_code, content={"status_code": res.status_code, "response": data})


@app.post("/ml/refresh-token")
def manual_refresh():
    return {"ok": refresh_ml_token()}


@app.get("/ml/probe-rapido")
def probe_rapido():
    """Sonda SOLO LECTURA: prueba si existe una vía RÁPIDA para traer el estado de
    envío de muchas órdenes en pocas llamadas (hoy es 1 request por shipment).
    Prueba: (a) si orders/search ya trae shipping.status, (b) variantes con
    x-format-new, (c) endpoints candidatos de búsqueda/multiget de shipments.
    No modifica nada; solo reporta qué responde ML."""
    out = {}
    seller_id = get_ml_seller_id()
    out["seller_id"] = seller_id
    if not seller_id:
        return {"ok": False, "error": "sin seller_id"}
    # (a) orders/search normal: ¿qué trae el campo shipping de cada orden?
    sample_order, shipping_id = None, None
    try:
        d = ml_get(f"https://api.mercadolibre.com/orders/search?seller={seller_id}&sort=date_desc&limit=3")
        res = d.get("results") or []
        if res:
            sample_order = str(res[0].get("id"))
            sh = res[0].get("shipping") or {}
            shipping_id = sh.get("id")
            out["a_orders_search_shipping_keys"] = sorted(sh.keys())
            out["a_trae_status"] = "status" in sh
            out["a_shipping_sample"] = {k: sh.get(k) for k in list(sh.keys())[:12]}
    except Exception as e:
        out["a_error"] = str(e)
    # (b) orders/search con x-format-new: ¿enriquece el shipping?
    try:
        d = ml_get(f"https://api.mercadolibre.com/orders/search?seller={seller_id}&sort=date_desc&limit=3",
                   {"x-format-new": "true"})
        res = d.get("results") or []
        if res:
            sh = res[0].get("shipping") or {}
            out["b_newformat_shipping_keys"] = sorted(sh.keys())
            out["b_trae_status"] = "status" in sh
    except Exception as e:
        out["b_error"] = str(e)
    # (c) candidatos de búsqueda/multiget de shipments (esperado: 404/400 si no existen)
    cands = []
    if shipping_id:
        cands.append(("multiget_shipments", f"https://api.mercadolibre.com/multiget/shipments?ids={shipping_id}"))
        cands.append(("shipments_ids", f"https://api.mercadolibre.com/shipments?ids={shipping_id}"))
    cands.append(("shipments_search_seller", f"https://api.mercadolibre.com/shipments/search?seller_id={seller_id}&limit=5"))
    if sample_order:
        cands.append(("orders_shipments", f"https://api.mercadolibre.com/orders/{sample_order}/shipments"))
    for nombre, url in cands:
        try:
            r = ml_get(url)
            out[f"c_{nombre}"] = {"ok": True, "tipo": type(r).__name__,
                                 "claves": sorted(r.keys())[:12] if isinstance(r, dict) else f"lista[{len(r)}]"}
        except Exception as e:
            out[f"c_{nombre}"] = {"ok": False, "error": str(e)[:160]}
    return {"ok": True, **out}


@app.get("/ml/debug-hoy")
def debug_hoy(limit: int = 120):
    """Diagnóstico SOLO LECTURA: trae las órdenes ML más recientes SIN filtrar por
    estado (la reconciliación solo busca status=paid), y las compara con la BD.
    Sirve para ver por qué faltan pedidos de hoy: qué estado traen, si ML las
    devuelve, y si están o no en la base (por id o por pack)."""
    import collections
    seller_id = get_ml_seller_id()
    if not seller_id:
        return {"ok": False, "error": "sin seller_id"}
    results = []
    offset = 0
    # Paginar SIN filtro de estado (todos), date_desc, hasta `limit`.
    while len(results) < limit:
        url = (f"https://api.mercadolibre.com/orders/search"
               f"?seller={seller_id}&sort=date_desc&limit=50&offset={offset}")
        try:
            data = ml_get(url)
        except Exception as e:
            return {"ok": False, "error": f"orders/search fallo: {e}", "traidas": len(results)}
        res = data.get("results") or []
        if not res:
            break
        results.extend(res)
        offset += 50
        if len(res) < 50:
            break
    results = results[:limit]
    ids = [str(o.get("id")) for o in results]
    packs = [str(o.get("pack_id")) for o in results if o.get("pack_id")]
    en_bd = set()
    if ids or packs:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids + packs,))
                en_bd = {str(r["id"]) for r in cur.fetchall()}
    por_estado = collections.Counter(o.get("status", "") for o in results)
    faltan = []
    detalle = []
    for o in results:
        oid = str(o.get("id"))
        pid = str(o.get("pack_id") or "")
        esta = (oid in en_bd) or (pid and pid in en_bd)
        row = {
            "id": oid, "pack_id": pid or None, "status": o.get("status"),
            "date_created": o.get("date_created"),
            "shipping_id": (o.get("shipping") or {}).get("id"),
            "en_bd": esta,
        }
        detalle.append(row)
        if not esta:
            faltan.append(row)
    return {
        "ok": True, "seller_id": seller_id, "traidas": len(results),
        "por_estado": dict(por_estado),
        "en_bd": sum(1 for d in detalle if d["en_bd"]),
        "faltan": len(faltan),
        "muestra_faltantes": faltan[:15],
        "muestra_detalle": detalle[:8],
    }


@app.post("/ml/backfill-shipping")
def backfill_shipping(dias: int = 4, limit: int = 100):
    """Backfill puntual del estado REAL de envío (estado_envio_real) para ventas ML
    viejas que aún no lo tienen (cargadas antes del webhook 'shipments'). SEGURO:
    recorre SECUENCIALMENTE y cada get_ml_shipment pasa por _ml_lock (mín 500ms
    entre requests) → NUNCA satura ML (lo contrario del barrido paralelo que dio
    429). Acotado por `dias` (ventana) y `limit` (tope). Usa el shipping.id que ya
    está en order_json → 1 fetch por venta, sin get_ml_order. Idempotente: solo
    toca las que tienen estado_envio_real vacío."""
    procesadas = 0
    actualizadas = 0
    sin_ship = 0
    errores = 0
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, order_json FROM ventas
                WHERE fuente = 'mercadolibre'
                  AND (estado_envio_real IS NULL OR estado_envio_real = '')
                  AND creado_en >= NOW() - make_interval(days => %s)
                ORDER BY creado_en DESC
                LIMIT %s
                """,
                (dias, limit),
            )
            rows = cur.fetchall()
    for r in rows:
        procesadas += 1
        try:
            oj = json.loads(r.get("order_json") or "{}")
            sid = (oj.get("shipping") or {}).get("id")
            if not sid:
                sin_ship += 1
                continue
            sh = get_ml_shipment(sid)  # 1 fetch, throttleado por _ml_lock
            st = (sh or {}).get("status") or ""
            sub = (sh or {}).get("substatus") or ""
            if st:
                update_venta(r["id"], estado_envio_real=st, estado_envio_sub=sub)
                actualizadas += 1
        except Exception as e:
            errores += 1
            logger.warning(f"[backfill] {r.get('id')}: {e}")
    logger.info(
        f"[backfill] dias={dias} limit={limit} -> procesadas={procesadas} "
        f"actualizadas={actualizadas} sin_ship={sin_ship} err={errores}"
    )
    return {
        "ok": True, "dias": dias, "limit": limit,
        "procesadas": procesadas, "actualizadas": actualizadas,
        "sin_shipping_id": sin_ship, "errores": errores,
    }


@app.get("/ventas")
def ventas(estado: Optional[str] = None, ids: Optional[str] = None,
           desde: Optional[str] = None, limit: Optional[int] = None,
           light: Optional[int] = 0):
    """Lista de ventas. Sin parámetros devuelve TODO (compatibilidad).
    Filtros para no pagar los ~4.6MB cuando no hace falta:
      ?ids=1,2,3     -> solo esas ventas (por id o pack_id)
      ?desde=2026-07-20 -> creadas desde esa fecha
      ?limit=200     -> tope de filas
      ?light=1       -> omite 'productos' (no parsea order_json; mucho más rápido)"""
    id_list = [s.strip() for s in ids.split(",") if s.strip()] if ids else None
    items = list_ventas(estado, ids=id_list, desde=desde, limit=limit)
    enriched = []
    if light:
        # Camino liviano: sin parsear order_json (lo pesado). Solo campos de la fila.
        for v in items:
            oj = v.pop("order_json", None)
            v.pop("billing_json", None)
            on = ""
            if oj:
                try:
                    on = (json.loads(oj) or {}).get("order_number") or ""
                except Exception:
                    on = ""
            enriched.append({**v, "tipo_envio": v.get("tipo_envio_ml") or "-", "order_number": on})
        return {"items": enriched}
    for v in items:
        order_number = ""
        try:
            order = json.loads(v.get("order_json") or "{}")
            productos, cantidad_items, total_bruto = summarize_order_items(order)
            order_number = order.get("order_number") or ""
        except Exception:
            productos, cantidad_items, total_bruto = [], 0, 0.0
        tipo_envio = v.get("tipo_envio_ml") or "-"
        v.pop("order_json", None)
        v.pop("billing_json", None)
        enriched.append({
            **v,
            "productos": productos,
            "cantidad_items": cantidad_items,
            "total_bruto": total_bruto,
            "tipo_envio": tipo_envio,
            "order_number": order_number,
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
    productos: Optional[list] = []
    total_bruto: Optional[float] = 0.0
    autorizar: bool = False


@app.post("/ventas/manual")
def ingresar_venta_manual(payload: VentaManualPayload):
    try:
        rut_norm = normalize_rut(payload.rut)
        oid = payload.order_id or f"MANUAL-{int(datetime.now().timestamp())}"
        giro_final = payload.giro or (DEFAULT_BOLETA_ACTIVITY if payload.tipo == "Boleta" else "")
        items = []
        for p in (payload.productos or []):
            items.append({"item": {"title": p}, "quantity": 1, "unit_price": 0})
        order_fake = {"id": oid, "status": "paid", "order_items": items, "buyer": {"email": payload.email}}
        billing_fake = {}
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = %s", (str(oid),))
                if cur.fetchone():
                    raise HTTPException(status_code=409, detail=f"Ya existe una venta con ID {oid}")
                cur.execute(
                    """INSERT INTO ventas
                        (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region,
                         tipo_sugerido, estado, estado_envio, order_json, billing_json, fuente)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', 'paid', %s, %s, 'manual')
                    """,
                    (
                        str(oid), None, payload.cliente.strip(), rut_norm,
                        payload.email.strip(), giro_final,
                        (payload.direccion or "").strip(), (payload.ciudad or "").strip(),
                        (payload.region or "").strip(), payload.tipo,
                        json.dumps(order_fake, ensure_ascii=False),
                        json.dumps(billing_fake, ensure_ascii=False),
                    )
                )
            conn.commit()
        logger.info(f"Venta manual ingresada: id={oid} cliente={payload.cliente} tipo={payload.tipo}")
        if payload.autorizar:
            move_id, partner_id = create_document(
                order=order_fake, billing_raw=billing_fake, tipo=payload.tipo,
                email=payload.email, giro=giro_final,
                cliente_override=payload.cliente, rut_override=rut_norm,
                direccion_override=payload.direccion, ciudad_override=payload.ciudad,
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


class AutorizarMasivoPayload(BaseModel):
    ids: list


@app.post("/ventas/autorizar-masivo")
def autorizar_masivo(payload: AutorizarMasivoPayload):
    """Emite en Odoo todas las ventas indicadas (facturacion manual masiva)."""
    ids = [str(i) for i in (payload.ids or [])]
    if not ids:
        raise HTTPException(status_code=400, detail="No se enviaron ventas")
    resultados = {"ok": 0, "error": 0, "detalle": []}
    for oid in ids:
        try:
            move_id, _ = emitir_venta(oid)
            resultados["ok"] += 1
            resultados["detalle"].append({"id": oid, "ok": True, "move_id": move_id})
        except SplitParentError:
            resultados["detalle"].append({"id": oid, "ok": True, "dividida": True,
                                          "message": "dividida: no se factura la original"})
        except Exception as e:
            resultados["error"] += 1
            resultados["detalle"].append({"id": oid, "ok": False, "error": str(e)[:200]})
            _v = get_venta(oid)
            manejar_error_emision(oid, (_v or {}).get("tipo_sugerido") or "Boleta", e)
    logger.info(f"Autorizacion masiva: {resultados['ok']} OK, {resultados['error']} errores")
    return resultados


@app.post("/ventas/actualizar-envio")
def actualizar_tipo_envio():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, order_json FROM ventas WHERE estado != 'enviado' AND (fuente IS NULL OR fuente = 'mercadolibre') ORDER BY creado_en DESC LIMIT 100")
                rows = cur.fetchall()
        if not rows:
            return {"ok": True, "actualizadas": 0, "mensaje": "No hay ventas para procesar"}
        def procesar_en_background():
            actualizadas = 0
            errores = 0
            lote = 10
            for i in range(0, len(rows), lote):
                chunk = rows[i:i+lote]
                for row in chunk:
                    try:
                        oid = str(row["id"])
                        order = json.loads(row.get("order_json") or "{}")
                        tipo_envio = extract_logistic_type(order)
                        if tipo_envio:
                            with get_db() as conn2:
                                with conn2.cursor() as cur2:
                                    cur2.execute("UPDATE ventas SET tipo_envio_ml = %s WHERE id = %s", (tipo_envio, oid))
                                conn2.commit()
                            actualizadas += 1
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Error actualizando envio {row['id']}: {e}")
                        errores += 1
                        time.sleep(3)
                wait = 0
                while webhook_queue.qsize() > 5 and wait < 120:
                    time.sleep(5)
                    wait += 5
            logger.info(f"Actualizacion tipo_envio: {actualizadas} OK, {errores} errores")
        t = threading.Thread(target=procesar_en_background, daemon=True)
        t.start()
        return {"ok": True, "procesando": len(rows), "mensaje": f"Actualizando tipo de envio en {len(rows)} ventas en background."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/reconciliar")
def reconciliar_manual():
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
        def encolar_en_lotes(lista):
            lote = 10
            for i in range(0, len(lista), lote):
                chunk = lista[i:i+lote]
                for oid in chunk:
                    if oid not in list(webhook_queue.queue):
                        webhook_queue.put(oid)
                wait = 0
                while webhook_queue.qsize() > 5 and wait < 120:
                    time.sleep(5)
                    wait += 5
        t = threading.Thread(target=encolar_en_lotes, args=(faltantes,), daemon=True)
        t.start()
        return {
            "ok": True, "ordenes_ml": len(ordenes_ml), "en_bd": len(ids_en_bd),
            "faltantes": len(faltantes),
            "mensaje": f"Procesando {len(faltantes)} ventas faltantes en background.",
            "encoladas": faltantes[:5]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ml/revisar-canceladas")
def ml_revisar_canceladas():
    """Revisa las ordenes ML canceladas recientes y crea la NC de las que ya estaban facturadas
    (cubre ventas divididas/canceladas DESPUES de emitir). Respeta NC_AUTO[ml][total]. Idempotente."""
    try:
        seller_id = get_ml_seller_id()
        if not seller_id:
            raise HTTPException(status_code=500, detail="No se pudo obtener seller_id de ML")
        def _run():
            n = revisar_canceladas_ml(seller_id)
            logger.info(f"[ML] revisar-canceladas manual completado: {n} evaluadas")
        threading.Thread(target=_run, daemon=True).start()
        return {"ok": True, "mensaje": "Revisando canceladas ML en background (NC de las ya facturadas)."}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/ingresar/{order_id}")
def ingresar_manual(order_id: str):
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
    todas = list_ventas("pendiente") + list_ventas("error")
    resultados = {"ok": 0, "error": 0, "errores": []}
    ml_ventas = [v for v in todas if (v.get("fuente") or "mercadolibre") == "mercadolibre"]
    logger.info(f"Reprocesar todo: {len(ml_ventas)} ventas ML a procesar")
    for i, venta in enumerate(ml_ventas):
        try:
            reprocesar_venta_desde_ml(str(venta["id"]))
            resultados["ok"] += 1
        except Exception as e:
            resultados["error"] += 1
            resultados["errores"].append({"id": venta["id"], "error": str(e)[:200]})
        # Pausa entre ventas para no saturar ML
        # El lock de ml_get ya serializa, pero damos espacio al webhook worker
        if i < len(ml_ventas) - 1:
            time.sleep(2 + min(_consecutive_429, 5))
    return resultados


class AgruparPayload(BaseModel):
    ids: list


@app.post("/ventas/agrupar")
def agrupar_ventas(payload: AgruparPayload):
    ids = [str(i) for i in payload.ids]
    if len(ids) < 2:
        raise HTTPException(status_code=400, detail="Se necesitan al menos 2 ventas para agrupar")
    ventas_list = []
    for oid in ids:
        v = get_venta(oid)
        if not v:
            raise HTTPException(status_code=404, detail=f"Venta {oid} no encontrada")
        if v.get("estado") == "enviado":
            raise HTTPException(status_code=400, detail=f"La venta {oid} ya fue enviada a Odoo")
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
        update_venta(principal["id"], order_json=json.dumps(order_consolidado, ensure_ascii=False))
        for v in secundarias:
            update_venta(v["id"], estado="rechazado", error=f"Agrupada en venta {principal['id']}")
        return {
            "ok": True, "venta_principal": principal["id"],
            "agrupadas": [v["id"] for v in secundarias],
            "total_items": item_count, "total_bruto": total_bruto,
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
    try:
        ctx = odoo_connect()
        chile_id = get_chile_country_id(ctx)
        rut_type_id = get_rut_id_type(ctx)
        activity_field = get_activity_field_name(ctx)
        rut_norm = normalize_rut(payload.rut)
        rut_odoo = rut_con_guion(rut_norm) if rut_norm else False
        taxpayer_type = "1" if payload.es_empresa else "4"
        existing = find_partner_by_rut(ctx, payload.rut)
        if existing:
            raise HTTPException(status_code=409, detail=f"Ya existe un cliente con ese RUT (id={existing})")
        vals = {
            "name": payload.nombre.strip(), "vat": rut_odoo,
            "email": payload.email.strip(), "l10n_cl_dte_email": payload.email.strip(),
            "customer_rank": 1, "company_type": "company" if payload.es_empresa else "person",
            "is_company": payload.es_empresa, "country_id": chile_id,
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
        partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
        logger.info(f"Cliente creado: id={partner_id} nombre={payload.nombre}")
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


@app.post("/ventas/{oid}/recalcular-wc")
def recalcular_wc_venta_endpoint(oid: str):
    """Recalcula una venta WooCommerce desde la web (arregla montos IVA + envio)."""
    try:
        data = reprocesar_wc_venta(str(oid))
        return {"ok": True, **data}
    except Exception as e:
        logger.error(f"[{oid}] Error recalculando WC: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ventas/{oid}/pack")
def ver_pack(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    pack_id = venta.get("pack_id")
    if not pack_id:
        try:
            order = json.loads(venta.get("order_json") or "{}")
            items, item_count, total = summarize_order_items(order)
            return {"pack_id": None, "ordenes": [{"id": venta["id"], "cliente": venta.get("cliente"), "total": total, "items": items, "item_count": item_count}]}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
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
            ordenes.append({"id": order_id, "status": order.get("status"), "total": total, "items": items, "item_count": item_count})
        return {"pack_id": pack_id, "total_pack": round(total_pack, 2), "ordenes": ordenes}
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
        move_id, partner_id = emitir_venta(oid)
        return {"ok": True, "id": oid, "move_id": move_id, "partner_id": partner_id}
    except SplitParentError as e:
        return {"ok": True, "id": oid, "dividida": True,
                "message": "Venta dividida en ML: no se factura la orden original; las ordenes hijas se facturan por separado"}
    except Exception as e:
        logger.error(f"[{oid}] Error al autorizar: {e}", exc_info=True)
        manejar_error_emision(oid, venta.get("tipo_sugerido") or "Boleta", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/{oid}/adjuntar-ml")
def adjuntar_ml_manual(oid: str):
    """Test manual en 1 orden ML: si no esta emitida la EMITE (crea boleta/factura en Odoo) y
    luego sube el PDF a Mercado Libre. Devuelve la respuesta cruda de ML."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if (venta.get("fuente") or "mercadolibre") != "mercadolibre":
        raise HTTPException(status_code=400, detail="Solo aplica a ventas de Mercado Libre")
    emitido_ahora = False
    move_id = venta.get("move_id")
    if not move_id or venta.get("estado") != "enviado":
        try:
            move_id, _ = emitir_venta(oid)
            emitido_ahora = True
        except SplitParentError:
            raise HTTPException(status_code=400, detail="Venta dividida en ML: no se factura la orden original; adjunta el PDF en cada orden hija por separado")
        except Exception as e:
            logger.error(f"[{oid}] Error emitiendo antes de adjuntar: {e}", exc_info=True)
            manejar_error_emision(oid, venta.get("tipo_sugerido") or "Boleta", e)
            raise HTTPException(status_code=500, detail=f"No se pudo emitir el DTE: {e}")
    try:
        resp = adjuntar_comprobante_ml(oid, move_id)
        return {"ok": True, "id": oid, "emitido_ahora": emitido_ahora, "move_id": move_id, "respuesta": resp}
    except Exception as e:
        logger.error(f"[{oid}] Error adjuntando a ML: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/{oid}/adjuntar-fl")
def adjuntar_fl_manual(oid: str):
    """Test manual en 1 orden Falabella: si no esta emitida la EMITE (crea boleta/factura
    en Odoo) y luego sube el PDF a Falabella (SetInvoicePDF). Devuelve la respuesta cruda."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if (venta.get("fuente") or "") != "falabella":
        raise HTTPException(status_code=400, detail="Solo aplica a ventas de Falabella")
    emitido_ahora = False
    move_id = venta.get("move_id")
    if not move_id or venta.get("estado") != "enviado":
        try:
            move_id, _ = emitir_venta(oid)
            emitido_ahora = True
        except Exception as e:
            logger.error(f"[{oid}] Error emitiendo antes de adjuntar a FL: {e}", exc_info=True)
            manejar_error_emision(oid, venta.get("tipo_sugerido") or "Boleta", e)
            raise HTTPException(status_code=500, detail=f"No se pudo emitir el DTE: {e}")
    try:
        resp = adjuntar_comprobante_fl(oid, move_id)
        return {"ok": True, "id": oid, "emitido_ahora": emitido_ahora, "move_id": move_id, "respuesta": resp}
    except Exception as e:
        logger.error(f"[{oid}] Error adjuntando a Falabella: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/{oid}/nota-credito")
def crear_nota_credito(oid: str, body: dict):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if venta.get("estado") != "enviado":
        raise HTTPException(status_code=400, detail="Solo se puede crear NC de ventas ya enviadas a Odoo")
    if not venta.get("move_id"):
        raise HTTPException(status_code=400, detail="La venta no tiene documento en Odoo")
    motivo = (body.get("motivo") or "").strip()
    if not motivo:
        raise HTTPException(status_code=400, detail="El motivo es obligatorio")
    try:
        nc_move_id = _crear_nota_credito(venta, motivo)
        return {"ok": True, "id": oid, "nc_move_id": nc_move_id, "motivo": motivo, "mensaje": "Nota de credito creada en Odoo"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{oid}] Error creando NC: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _fl_devueltos_por_titulo(order_id: str) -> dict:
    """Para Falabella: {titulo_item: cantidad_devuelta/cancelada} segun GetOrderItems.
    Sirve para sugerir las lineas a acreditar en una NC parcial."""
    rev = _fl_estados_reversion()
    out = {}
    try:
        for it in fl_get_order_items(order_id):
            st = str(it.get("Status") or "").strip().lower().replace(" ", "_")
            if st in rev:
                name = str(it.get("Name") or it.get("SellerSku") or "").strip()
                out[name] = out.get(name, 0) + float(it.get("Quantity") or it.get("QtyOrdered") or 1)
    except Exception as e:
        logger.warning(f"[FL:{order_id}] no se pudo sugerir devueltos: {e}")
    return out


@app.get("/ventas/{oid}/lineas-dte")
def lineas_dte(oid: str):
    """Lineas del documento emitido + cantidad SUGERIDA a acreditar (devueltos del marketplace).
    Alimenta el modal de NC parcial."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if not venta.get("move_id"):
        raise HTTPException(status_code=400, detail="La venta no tiene documento en Odoo")
    try:
        lineas = obtener_lineas_dte_odoo(venta["move_id"])
    except Exception as e:
        logger.error(f"[{oid}] Error leyendo lineas DTE: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    # Sugerencia de devueltos (solo Falabella por ahora; ML queda manual)
    sugeridos = {}
    if (venta.get("fuente") or "") == "falabella":
        sugeridos = _fl_devueltos_por_titulo(oid.replace("FL-", "", 1))
    for ln in lineas:
        sug = 0
        for name, qty in sugeridos.items():
            if name and (name == ln["name"] or name in ln["name"] or ln["name"] in name):
                sug = min(qty, ln["quantity"])
                break
        ln["sugerido"] = sug
    return {"ok": True, "id": oid, "lineas": lineas}


@app.post("/ventas/{oid}/nota-credito-parcial")
def crear_nota_credito_parcial(oid: str, body: dict):
    """Crea una NC PARCIAL acreditando solo los items/cantidades indicados; la factura original
    queda vigente. body = {motivo, lineas:[{line_index, cantidad}]}."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    if venta.get("estado") != "enviado":
        raise HTTPException(status_code=400, detail="Solo se puede crear NC de ventas ya enviadas a Odoo")
    if not venta.get("move_id"):
        raise HTTPException(status_code=400, detail="La venta no tiene documento en Odoo")
    motivo = (body.get("motivo") or "").strip()
    if not motivo:
        raise HTTPException(status_code=400, detail="El motivo es obligatorio")
    creditos = body.get("lineas") or []
    if not creditos:
        raise HTTPException(status_code=400, detail="Debes indicar al menos un item con cantidad a acreditar")
    try:
        nc_id = _crear_nota_credito_parcial(venta, creditos, motivo)
        return {"ok": True, "id": oid, "nc_move_id": nc_id, "motivo": motivo,
                "mensaje": "Nota de credito PARCIAL creada; la factura original sigue vigente"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{oid}] Error creando NC parcial: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/ventas/{oid}/anular")
def anular_venta(oid: str):
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    move_id = venta.get("move_id")
    odoo_result = None
    if move_id:
        try:
            ctx = odoo_connect()
            moves = odoo_exec(ctx, "account.move", "search", [[["id", "=", move_id]]])
            if moves:
                state = odoo_exec(ctx, "account.move", "read", [moves], {"fields": ["state"]})[0]["state"]
                if state == "posted":
                    odoo_exec(ctx, "account.move", "button_cancel", [moves])
                    odoo_result = f"Documento {move_id} cancelado en Odoo"
                elif state == "cancel":
                    odoo_result = f"Documento {move_id} ya estaba cancelado"
                else:
                    odoo_result = f"Documento {move_id} en estado {state}"
        except Exception as e:
            odoo_result = f"No se pudo cancelar en Odoo: {e}"
            logger.warning(f"[{oid}] Error cancelando en Odoo: {e}")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE ventas SET estado='pendiente', move_id=NULL, partner_id=NULL, error=NULL, enviado_en=NULL WHERE id=%s",
                (oid,)
            )
        conn.commit()
    logger.info(f"[{oid}] Venta anulada y reseteada a pendiente. Odoo: {odoo_result}")
    return {"ok": True, "id": oid, "odoo": odoo_result, "message": "Venta reseteada a pendiente"}


class EstadoPayload(BaseModel):
    estado: str


ESTADOS_VALIDOS_MANUAL = {"pendiente", "enviado", "error", "rechazado", "nota_credito", "dividida"}


@app.post("/ventas/{oid}/estado")
def cambiar_estado_manual(oid: str, payload: EstadoPayload):
    """Cambia el estado de una venta manualmente (override). NO toca Odoo;
    solo actualiza el registro local. Util para corregir estados desfasados."""
    venta = get_venta(oid)
    if not venta:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    nuevo = (payload.estado or "").strip()
    if nuevo not in ESTADOS_VALIDOS_MANUAL:
        raise HTTPException(status_code=400, detail=f"Estado invalido. Validos: {sorted(ESTADOS_VALIDOS_MANUAL)}")
    update_venta(oid, estado=nuevo)
    logger.info(f"[{oid}] Estado cambiado manualmente a '{nuevo}'")
    return {"ok": True, "id": oid, "estado": nuevo}



# =========================
# DASHBOARD UI
# =========================


# =========================
# ENDPOINTS FALABELLA
# =========================

@app.post("/fl/webhook")
async def fl_webhook_endpoint(request: Request):
    """Recibe webhooks de Falabella Seller Center."""
    body_bytes = await request.body()
    signature  = request.headers.get("X-Hub-Signature", "")
    topic      = request.headers.get("X-Webhook-Event", "")

    if not verify_fl_webhook(body_bytes, signature):
        logger.warning(f"[FL] Firma invalida en webhook topic={topic}")
        return JSONResponse(status_code=401, content={"error": "Firma invalida"})

    try:
        body = json.loads(body_bytes)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body invalido"})

    # El payload de Falabella webhooks viene con el OrderId
    order_id = str(body.get("orderId") or body.get("OrderId") or body.get("order_id") or "")
    if not order_id:
        return {"ok": True, "ignored": True}

    if order_id not in list(fl_webhook_queue.queue):
        fl_webhook_queue.put(order_id)
        logger.info(f"[FL] Webhook encolado: order_id={order_id}")

    return {"ok": True, "order_id": order_id, "queued": fl_webhook_queue.qsize()}


@app.post("/fl/reconciliar")
def fl_reconciliar_manual(days: int = None, limit: int = 100):
    """Fuerza reconciliacion con Falabella: consulta las ordenes recientes e ingresa las
    que falten en la BD. Opcional: days = cuantos dias hacia atras buscar (por defecto usa
    FL_RECON_DAYS); limit = cuantas ordenes traer (max util 100)."""
    try:
        created_after = None
        if days and int(days) > 0:
            from datetime import timedelta
            created_after = (datetime.utcnow() - timedelta(days=int(days))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ordenes = fl_get_orders_recent(created_after=created_after, limit=limit)
        ids_fl  = [f"FL-{o['OrderId']}" for o in ordenes if o.get("OrderId")]
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_fl,))
                ids_en_bd = {str(row["id"]) for row in cur.fetchall()}
        faltantes = [o for o in ordenes if f"FL-{o['OrderId']}" not in ids_en_bd]
        faltantes_ids = [str(o["OrderId"]) for o in faltantes]
        def procesar_faltantes():
            for o in faltantes:
                try:
                    process_fl_order(str(o["OrderId"]), order_data=o)
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"[FL] Error en reconciliar manual {o['OrderId']}: {e}")
                    time.sleep(2)
            logger.info(f"[FL] Reconciliacion manual completada: {len(faltantes)} procesadas")
        threading.Thread(target=procesar_faltantes, daemon=True).start()
        return {
            "ok": True, "total_fl": len(ordenes), "en_bd": len(ids_en_bd),
            "faltantes": len(faltantes_ids), "encoladas": faltantes_ids[:5],
            "mensaje": f"Procesando {len(faltantes_ids)} ordenes Falabella faltantes en background."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fl/ingresar/{order_id}")
def fl_ingresar_manual(order_id: str):
    """Fuerza la ingesta de una orden Falabella por su ID."""
    try:
        process_fl_order(order_id)
        venta = get_venta(f"FL-{order_id}")
        if venta:
            return {"ok": True, "id": f"FL-{order_id}", "cliente": venta.get("cliente"), "estado": venta.get("estado")}
        return {"ok": True, "id": f"FL-{order_id}", "message": "Procesado, verificar en dashboard"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fl/reprocesar-datos/{order_id}")
def fl_reprocesar_datos_una(order_id: str):
    """Reprocesa 1 venta Falabella: actualiza envio + telefono desde la orden real."""
    oid = f"FL-{order_id}" if not str(order_id).startswith("FL-") else str(order_id)
    try:
        res = fl_reingesta_datos(oid)
        return {"ok": True, **res}
    except Exception as e:
        logger.error(f"[{oid}] reprocesar-datos fallo: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fl/reprocesar-datos")
def fl_reprocesar_datos_todos():
    """Reprocesa TODAS las ventas Falabella existentes: reconsulta cada orden y actualiza
    envio + telefono (y el telefono del partner en Odoo si ya existe). No modifica DTEs ya
    emitidos, pero deja las pendientes listas para emitir con envio + telefono correctos."""
    ventas_fl = [v for v in list_ventas(None) if (v.get("fuente") == "falabella")]
    ids = [v["id"] for v in ventas_fl]
    def _run():
        ok = err = 0
        for oid in ids:
            try:
                fl_reingesta_datos(oid)
                ok += 1
            except Exception as e:
                err += 1
                logger.error(f"[{oid}] reprocesar-datos (masivo) fallo: {e}")
            time.sleep(1)
        logger.info(f"[FL] reprocesar-datos masivo completado: ok={ok} err={err} total={len(ids)}")
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "total": len(ids),
            "mensaje": f"Reprocesando {len(ids)} ventas Falabella en background (envio + telefono)."}


@app.post("/fl/revisar-devoluciones")
def fl_revisar_devoluciones():
    """Revisa las ventas Falabella EMITIDAS y crea NC en las que la orden fue cancelada o
    devuelta TOTALMENTE en Falabella (idempotente). Corre en background. Las devoluciones
    parciales quedan registradas en el log para NC manual."""
    ventas_fl = [v for v in list_ventas(None)
                 if v.get("fuente") == "falabella" and v.get("estado") == "enviado" and v.get("move_id")]
    ids = [v["id"] for v in ventas_fl]
    def _run():
        nc = 0
        for oid in ids:
            try:
                fl_auto_nc(get_venta(oid))  # total o parcial segun items (respeta toggles)
                nc += 1
            except Exception as e:
                logger.error(f"[{oid}] revisar-devoluciones fallo: {e}")
            time.sleep(1)
        logger.info(f"[FL] revisar-devoluciones completado: {nc} NC creadas de {len(ids)} emitidas")
    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "total_emitidas": len(ids),
            "mensaje": f"Revisando {len(ids)} ventas FL emitidas (cancelaciones/devoluciones) en background."}


@app.post("/fl/revisar-devoluciones/{order_id}")
def fl_revisar_devolucion_una(order_id: str):
    """Revisa 1 venta Falabella emitida y crea NC si la orden fue cancelada/devuelta total."""
    oid = f"FL-{order_id}" if not str(order_id).startswith("FL-") else str(order_id)
    v = get_venta(oid)
    if not v:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    fl_auto_nc(v)  # decide total o parcial segun items (respeta toggles NC_AUTO)
    return {"ok": True, "id": oid, "mensaje": "Evaluada; si correspondia se creo la NC (ver estado/nc_motivo)"}


@app.get("/fl/debug/{order_id}")
def fl_debug_order(order_id: str):
    """Muestra los datos crudos de una orden Falabella para diagnostico."""
    try:
        order = fl_get_order(order_id)
        items = fl_get_order_items(order_id)
        extra_str = order.get("ExtraBillingAttributes") or ""
        billing   = fl_parse_extra_billing(extra_str)
        tipo      = fl_extract_tipo(order)
        return {
            "order_id":    order_id,
            "tipo":        tipo,
            "rut":         fl_extract_rut(order, tipo, billing),
            "nombre":      fl_extract_nombre(order, tipo, billing),
            "giro":        fl_extract_giro(tipo, billing),
            "email":       fl_extract_email(tipo, billing, order),
            "direccion":   fl_extract_direccion(tipo, billing, order),
            "ciudad":      fl_extract_ciudad(tipo, billing, order),
            "region":      fl_extract_region(tipo, billing, order),
            "InvoiceRequired": order.get("InvoiceRequired"),
            "ExtraBillingAttributes": billing,
            "NationalRegistrationNumber": order.get("NationalRegistrationNumber"),
            "raw_order":   order,
            "items_count": len(items),
            "items":       items[:3],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fl/buscar/{numero}")
def fl_buscar(numero: str, days: int = 30, ingresar: bool = False):
    """Busca una orden Falabella por OrderId O por OrderNumber (el 'Orden Nº' visible) en los
    ultimos 'days' dias y explica por que no aparece en el listado. Con ?ingresar=true la ingesta
    si falta. Ej: /fl/buscar/3243802464?days=60  /fl/buscar/3243802464?ingresar=true"""
    from datetime import timedelta
    numero = str(numero).strip().replace("FL-", "")
    created_after = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    try:
        ordenes = fl_get_orders_recent(created_after=created_after, limit=100)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    def _status(o):
        s = o.get("Statuses")
        vals = s.get("Status") if isinstance(s, dict) else s
        if isinstance(vals, list):
            vals = vals[0] if vals else ""
        if isinstance(vals, dict):
            vals = vals.get("Status")
        return str(vals or "").strip().lower().replace(" ", "_")

    match = None
    for o in ordenes:
        if str(o.get("OrderId")) == numero or str(o.get("OrderNumber") or "") == numero:
            match = o
            break
    if not match:
        return {"ok": True, "encontrada_en_fl": False, "revisadas": len(ordenes), "days": days,
                "nota": f"No aparece entre las ultimas {days} dias (max 100 ordenes). Prueba mas dias "
                        f"(?days=90). Verifica que el numero sea el OrderId o el OrderNumber (Orden Nº) real."}
    order_id = str(match.get("OrderId"))
    order_number = str(match.get("OrderNumber") or "")
    st = _status(match)
    oid = f"FL-{order_id}"
    v = get_venta(oid)
    resp = {"ok": True, "encontrada_en_fl": True, "order_id": order_id, "order_number": order_number,
            "status": st, "status_valido_para_facturar": st in FL_ESTADOS_VALIDOS,
            "en_bd": bool(v), "estado_bd": (v or {}).get("estado"), "motivo_no_aparece": None}
    if not v:
        if st not in FL_ESTADOS_VALIDOS:
            resp["motivo_no_aparece"] = (f"Estado '{st}' no esta en el filtro de facturacion "
                                         f"{sorted(FL_ESTADOS_VALIDOS)}; por eso se ignora al ingestar.")
        else:
            resp["motivo_no_aparece"] = ("Estado valido pero aun no ingresada: backlog del reconciliador "
                                         "(procesa por lotes cada 25 min) o webhook no configurado. Usa ?ingresar=true.")
    if ingresar and not v:
        try:
            process_fl_order(order_id)  # sin order_data -> trae datos completos (GetOrder)
            v2 = get_venta(oid)
            resp["ingresada"] = bool(v2)
            resp["estado_bd"] = (v2 or {}).get("estado")
            if not v2:
                resp["nota_ingesta"] = "No se guardo (probablemente el estado no es facturable)."
        except Exception as e:
            resp["error_ingesta"] = str(e)
    return resp


@app.get("/fl/debug-orders")
def fl_debug_orders(days: int = 30, limit: int = 20):
    """Diagnostico: respuesta CRUDA de GetOrders para ver si Falabella devuelve ordenes,
    un error de permisos, o datos enmascarados. Ej: /fl/debug-orders?days=60"""
    from datetime import timedelta
    created_after = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    try:
        data = fl_get("GetOrders", {"CreatedAfter": created_after, "Limit": str(limit),
                                    "SortBy": "created_at", "SortDirection": "DESC"})
    except Exception as e:
        return {"created_after": created_after, "error": str(e)}
    orders = []
    try:
        o = data["SuccessResponse"]["Body"]["Orders"]["Order"]
        orders = o if isinstance(o, list) else [o]
    except Exception:
        pass
    return {
        "created_after": created_after,
        "user": get_env("FL_USER_ID", required=False, default=""),
        "count": len(orders),
        "claves_respuesta": list(data.keys()) if isinstance(data, dict) else str(type(data)),
        "head_ids": [str(x.get("OrderId")) for x in orders[:10] if isinstance(x, dict)],
        "primeras_ordenes": orders[:3],
        "raw": data,
    }


# =========================
# FALABELLA SELLER CENTER
# =========================

FL_BASE_URL      = "https://sellercenter-api.falabella.com"
# Endpoint REST para cargar el documento tributario (SetInvoicePDF). Configurable por si
# cambia la ruta/version. Ref: developers.falabella.com .../reference/setinvoicepdf
FL_INVOICE_PDF_URL = os.getenv("FL_INVOICE_PDF_URL",
                               "https://sellercenter-api.falabella.com/v1/marketplace-sellers/invoice/pdf")
# Codigo de operador segun pais (Chile=FACL, Colombia=FACO, Peru=FAPE).
FL_OPERATOR_CODE = os.getenv("FL_OPERATOR_CODE", "FACL")
FL_DEFAULT_EMAIL = "boleta@lemulux.com"
FL_ESTADOS_VALIDOS = {"pending", "ready_to_ship", "shipped", "delivered", "processing"}

WC_STATE_TO_REGION_FL = {
    "AI": "Aysen del Gral. Carlos Ibanez del Campo",
    "AN": "Antofagasta", "AP": "Arica y Parinacota",
    "AR": "de la Araucania", "AT": "Atacama", "BI": "del BioBio",
    "CO": "Coquimbo", "LI": "del Libertador Gral. Bernardo O'Higgins",
    "LL": "de los Lagos", "LR": "Los Rios", "MA": "Magallanes",
    "ML": "del Maule", "NB": "del Nuble", "RM": "Metropolitana",
    "TA": "Tarapaca", "VS": "Valparaiso",
}

fl_webhook_queue = queue_module.Queue()


def fl_sign(params: dict) -> str:
    """Genera firma HMAC-SHA256 para Falabella Seller Center.
    Los valores deben ir URL-encoded antes de firmar, de lo contrario
    el @ del UserID y el + del Timestamp generan firma incorrecta.
    """
    from urllib.parse import quote
    api_key = get_env("FL_API_KEY")
    encoded = []
    for k, v in sorted(params.items()):
        encoded.append(f"{quote(str(k), safe='')}={quote(str(v), safe='')}")
    query_string = "&".join(encoded)
    return hmac.new(api_key.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()


def fl_get(action: str, extra_params: dict = None) -> dict:
    """Llama a la API de Falabella Seller Center con firma HMAC."""
    with _ml_lock:
        elapsed = time.time() - _ml_last_request
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        # Esta cuenta responde con Version 1.0 en todas las acciones (GetOrders v1.0 trae todo,
        # incluido NationalRegistrationNumber/RUT y ExtraBillingAttributes). La v2.0 devolvia 400.
        # Configurable con FL_API_VERSION por si cambia.
        version = os.getenv("FL_API_VERSION", "1.0")
        params = {
            "Action":    action,
            "Format":    "JSON",
            "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "UserID":    get_env("FL_USER_ID"),
            "Version":   version,
        }
        if extra_params:
            params.update(extra_params)

        params["Signature"] = fl_sign(params)

        try:
            res = requests.get(
                FL_BASE_URL,
                params=params,
                headers={"User-Agent": f"{get_env('FL_USER_ID')}/Python/3", "accept": "application/json"},
                timeout=30,
            )
            if res.status_code == 429:
                logger.warning(f"[FL] 429 en {action}, esperando 30s")
                time.sleep(30)
                params["Timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
                params["Signature"] = fl_sign(params)
                res = requests.get(FL_BASE_URL, params=params,
                    headers={"User-Agent": f"{get_env('FL_USER_ID')}/Python/3", "accept": "application/json"},
                    timeout=30)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"[FL] Error en {action}: {e}")
            raise


def _fl_extract_list(data: dict, container_key: str, item_key: str) -> list:
    """Extrae la lista de items (Orders/OrderItems) del JSON de Falabella.
    En JSON, Body[container] es una LISTA de {item_key: {...}}; en XML seria un dict
    {item_key: {...} o [...]}. Se manejan ambas formas."""
    try:
        cont = data["SuccessResponse"]["Body"][container_key]
    except (KeyError, TypeError):
        return []
    if isinstance(cont, list):
        out = []
        for x in cont:
            out.append(x[item_key] if isinstance(x, dict) and item_key in x else x)
        return out
    if isinstance(cont, dict):
        it = cont.get(item_key)
        if isinstance(it, list):
            return it
        if it:
            return [it]
    return []


def fl_get_order(order_id: str) -> dict:
    """GetOrder devuelve datos del cliente + billing de la orden."""
    data = fl_get("GetOrder", {"OrderId": str(order_id)})
    orders = _fl_extract_list(data, "Orders", "Order")
    return orders[0] if orders else {}


def fl_get_order_items(order_id: str) -> list:
    """GetOrderItems devuelve los productos de la orden."""
    data = fl_get("GetOrderItems", {"OrderId": str(order_id)})
    return _fl_extract_list(data, "OrderItems", "OrderItem")


def fl_order_item_ids(order_id: str) -> list:
    """Lista de OrderItemId de una orden Falabella (necesarios para SetInvoicePDF)."""
    ids = []
    for it in fl_get_order_items(order_id):
        oii = it.get("OrderItemId") or it.get("OrderItemID") or it.get("order_item_id")
        if oii is not None and str(oii).strip():
            ids.append(str(oii).strip())
    return ids


def fl_invoice_type(tipo: str) -> str:
    """Mapea el tipo interno (Boleta/Factura/Nota de credito) al enum de Falabella."""
    t = (tipo or "").strip().lower()
    if t.startswith("factura"):
        return "FACTURA"
    if "credito" in t or "crédito" in t or t.startswith("nc") or "nota" in t:
        return "NOTA_DE_CREDITO"
    return "BOLETA"


def subir_comprobante_fl(order_item_ids: list, pdf_bytes: bytes, invoice_number: str,
                         invoice_date: str, invoice_type: str, oid: str) -> dict:
    """Sube el PDF del documento tributario a Falabella Seller Center (SetInvoicePDF).
    POST {FL_INVOICE_PDF_URL} con los parametros comunes firmados en headers y el
    payload (incluido el PDF en base64) en el body JSON. Solo aplica a ordenes que ya
    alcanzaron 'ready_to_ship'; no aplica a items despachados por Falabella (FBF)."""
    if not pdf_bytes:
        raise Exception("PDF vacio")
    if not order_item_ids:
        raise Exception("La orden no tiene OrderItemIds para asociar el documento")
    if not invoice_number:
        raise Exception("Falta el numero (folio) del documento emitido en Odoo")

    version = os.getenv("FL_API_VERSION", "1.0")
    # La firma va sobre los parametros comunes (Action, Format, Service, Timestamp,
    # UserID, Version), ordenados y URL-encoded. Reutiliza fl_sign (mismo algoritmo).
    common = {
        "Action":    "SetInvoicePDF",
        "Format":    "JSON",
        "Service":   "Invoice",
        "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "UserID":    get_env("FL_USER_ID"),
        "Version":   version,
    }
    common["Signature"] = fl_sign(common)

    body = {
        "orderItemIds":          [str(i) for i in order_item_ids],
        "invoiceNumber":         str(invoice_number),
        "invoiceDate":           str(invoice_date),
        "invoiceType":           invoice_type,
        "operatorCode":          FL_OPERATOR_CODE,
        "invoiceDocumentFormat": "pdf",
        "invoiceDocument":       base64.b64encode(pdf_bytes).decode("ascii"),
    }
    headers = dict(common)
    headers["Content-Type"] = "application/json"
    headers["accept"] = "application/json"
    headers["User-Agent"] = f"{get_env('FL_USER_ID')}/Python/3"

    with _ml_lock:
        elapsed = time.time() - _ml_last_request
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        res = requests.post(FL_INVOICE_PDF_URL, params=common, headers=headers,
                            json=body, timeout=90)
    if res.status_code == 429:
        logger.warning(f"[FL] 429 en SetInvoicePDF ({oid}), esperando 30s")
        time.sleep(30)
        common["Timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        common["Signature"] = fl_sign({k: v for k, v in common.items() if k != "Signature"})
        headers.update({k: common[k] for k in ("Timestamp", "Signature")})
        res = requests.post(FL_INVOICE_PDF_URL, params=common, headers=headers,
                            json=body, timeout=90)
    try:
        data = res.json()
    except Exception:
        data = {"status_code": res.status_code, "text": res.text[:500]}
    # Duplicado = documento ya cargado -> idempotente, se trata como OK.
    _txt = json.dumps(data).lower() if isinstance(data, (dict, list)) else str(data).lower()
    if res.status_code == 409 or "duplicat" in _txt or "already" in _txt or "e004" in _txt or "ya existe" in _txt:
        logger.info(f"[{oid}] Falabella: el documento YA estaba cargado (ok idempotente): {data}")
        return {"ok": True, "ya_cargado": True, "status_code": res.status_code}
    if res.status_code >= 400 or (isinstance(data, dict) and data.get("ErrorResponse")):
        raise Exception(f"Falabella rechazo el documento (HTTP {res.status_code}): {data}")
    return data


def adjuntar_comprobante_fl(oid: str, move_id: int) -> dict:
    """Sube a Falabella el PDF del DTE ya emitido en Odoo. Solo canal Falabella."""
    venta = get_venta(oid)
    if not venta:
        raise Exception("Venta no encontrada")
    if (venta.get("fuente") or "") != "falabella":
        raise Exception("Solo aplica a ventas de Falabella")
    order = json.loads(venta.get("order_json") or "{}")
    order_id = str(oid).replace("FL-", "", 1)
    item_ids = fl_order_item_ids(order_id)
    datos = obtener_datos_dte_odoo(move_id)
    pdf = obtener_pdf_dte_odoo(move_id)
    inv_type = fl_invoice_type(venta.get("tipo_sugerido") or "Boleta")
    resp = subir_comprobante_fl(item_ids, pdf, datos["numero"], datos["fecha"], inv_type, oid)
    logger.info(f"[{oid}] Documento tributario subido a Falabella "
                f"(items {item_ids}, folio {datos['numero']}, {inv_type}, {len(pdf)} bytes): {resp}")
    return resp


def fl_get_orders_recent(created_after: str = None, limit: int = 100) -> list:
    """GetOrders listado de ordenes recientes para reconciliacion.
    Ventana por defecto amplia (FL_RECON_DAYS, 10 dias) porque una orden se crea dias antes
    de estar lista para facturar."""
    if not created_after:
        from datetime import timedelta
        days = int(os.getenv("FL_RECON_DAYS", "10"))
        created_after = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    data = fl_get("GetOrders", {
        "CreatedAfter": created_after,
        "Limit": str(limit),
        "SortBy": "created_at",
        "SortDirection": "DESC",
    })
    return _fl_extract_list(data, "Orders", "Order")


def fl_parse_extra_billing(extra_str: str) -> dict:
    """Parsea ExtraBillingAttributes que viene como JSON string."""
    if not extra_str:
        return {}
    try:
        if isinstance(extra_str, dict):
            return extra_str
        return json.loads(extra_str)
    except Exception:
        return {}


def fl_extract_tipo(order: dict) -> str:
    """InvoiceRequired=true -> Factura, false/ausente -> Boleta."""
    val = str(order.get("InvoiceRequired", "false")).lower()
    return "Factura" if val in ("true", "1") else "Boleta"


def fl_extract_rut(order: dict, tipo: str, billing: dict) -> str:
    """RUT desde LegalId+CustomerVerifierDigit (factura) o NationalRegistrationNumber (boleta)."""
    if tipo == "Factura":
        legal_id = str(billing.get("LegalId") or "").strip()
        verifier  = str(billing.get("CustomerVerifierDigit") or "").strip()
        if legal_id:
            # LegalId a veces ya incluye el digito verificador con guion
            if "-" in legal_id:
                rut_raw = legal_id
            elif verifier:
                rut_raw = f"{legal_id}-{verifier}"
            else:
                rut_raw = legal_id
            return normalize_rut(rut_raw)
    # Boleta: NationalRegistrationNumber ya viene con guion (ej: "16316358-6")
    rut = str(order.get("NationalRegistrationNumber") or "").strip()
    return normalize_rut(rut) if rut else ""


def fl_extract_nombre(order: dict, tipo: str, billing: dict) -> str:
    if tipo == "Factura":
        name = html_module.unescape(str(billing.get("ReceiverLegalName") or "")).strip()
        if name:
            return name
    first = html_module.unescape(str(order.get("CustomerFirstName") or "")).strip()
    last  = html_module.unescape(str(order.get("CustomerLastName") or "")).strip()
    return f"{first} {last}".strip() or "Cliente FL"


def fl_extract_giro(tipo: str, billing: dict) -> str:
    if tipo == "Factura":
        regimen = html_module.unescape(str(billing.get("ReceiverTypeRegimen") or "")).strip()
        return regimen or ""
    return DEFAULT_BOLETA_ACTIVITY


def _es_email_valido(s: str) -> bool:
    s = str(s or "").strip()
    return "@" in s and "." in s.split("@")[-1] and " " not in s


def fl_extract_email(tipo: str, billing: dict, order: dict) -> str:
    """Email del comprador (boleta o factura). Busca en billing (ReceiverEmail/Email),
    nivel orden (CustomerEmail/Email) y ambas direcciones; si nada, escanea la orden por un
    email valido que no sea el default.
    IMPORTANTE: si Falabella NO entrega email real, devuelve "" (vacio). NO se usa
    boleta@lemulux.com porque ese buzon lo recibimos nosotros: preferimos dejar el cliente
    sin correo en Odoo antes que auto-enviarnoslo."""
    billing = billing or {}
    order = order or {}
    addr_b = order.get("AddressBilling") if isinstance(order.get("AddressBilling"), dict) else {}
    addr_s = order.get("AddressShipping") if isinstance(order.get("AddressShipping"), dict) else {}
    candidatos = [
        billing.get("ReceiverEmail"), billing.get("Email"), billing.get("CustomerEmail"),
        order.get("CustomerEmail"), order.get("Email"), order.get("BuyerEmail"),
        addr_b.get("Email"), addr_b.get("CustomerEmail"),
        addr_s.get("Email"), addr_s.get("CustomerEmail"),
    ]
    for c in candidatos:
        c = str(c or "").strip()
        if _es_email_valido(c):
            return c
    # Ultimo recurso: escanear la orden completa por un email valido distinto del default.
    default_dom = FL_DEFAULT_EMAIL.split("@")[-1].lower()
    for s in flatten_strings(order):
        for token in s.replace(",", " ").replace(";", " ").split():
            if _es_email_valido(token) and token.split("@")[-1].lower() != default_dom:
                return token.strip()
    return ""  # sin email real -> vacio (no usar el default que recibimos nosotros)


def fl_extract_direccion(tipo: str, billing: dict, order: dict) -> str:
    if tipo == "Factura":
        addr = html_module.unescape(str(billing.get("ReceiverAddress") or "")).strip()
        if addr:
            return addr
    # Fallback: AddressShipping
    ship = order.get("AddressShipping") or {}
    if isinstance(ship, dict):
        parts = [
            html_module.unescape(str(ship.get("Address1") or "")).strip(),
            html_module.unescape(str(ship.get("Address2") or "")).strip(),
        ]
        return ", ".join(p for p in parts if p)
    return ""


def fl_extract_ciudad(tipo: str, billing: dict, order: dict) -> str:
    if tipo == "Factura":
        return html_module.unescape(str(billing.get("ReceiverMunicipality") or "")).strip()
    ship = order.get("AddressShipping") or {}
    if isinstance(ship, dict):
        return html_module.unescape(str(ship.get("Ward") or ship.get("City") or "")).strip()
    return ""


def fl_extract_region(tipo: str, billing: dict, order: dict) -> str:
    if tipo == "Factura":
        region = html_module.unescape(str(billing.get("ReceiverRegion") or "")).strip()
        if region.upper() in WC_STATE_TO_REGION_FL:
            return WC_STATE_TO_REGION_FL[region.upper()]
        return region
    ship = order.get("AddressShipping") or {}
    if isinstance(ship, dict):
        # Falabella devuelve Region como nombre completo en AddressShipping
        region = html_module.unescape(str(ship.get("Region") or "")).strip()
        return region
    return ""


def fl_extract_telefono(order: dict, billing: dict) -> str:
    """Telefono del cliente Falabella. Busca en el billing (ExtraBillingAttributes),
    luego en AddressBilling/AddressShipping, y por ultimo a nivel orden."""
    billing = billing or {}
    for k in ("ReceiverPhone", "Phone", "PhoneNumber", "ReceiverPhoneNumber", "ReceiverContactNumber"):
        v = str(billing.get(k) or "").strip()
        if v:
            return v
    for addr_key in ("AddressBilling", "AddressShipping"):
        addr = order.get(addr_key) or {}
        if isinstance(addr, dict):
            for k in ("Phone", "Phone2", "PhoneNumber", "MobilePhone"):
                v = str(addr.get(k) or "").strip()
                if v:
                    return v
    for k in ("CustomerPhone", "Phone", "PhoneNumber"):
        v = str(order.get(k) or "").strip()
        if v:
            return v
    return ""


def fl_extract_envio(items_raw: list, order: dict) -> float:
    """Costo de envio BRUTO que pago el comprador. En Falabella viene por item
    (ShippingAmount); se suma para toda la orden. Fallback a nivel orden si aplica."""
    envio = 0.0
    for it in (items_raw or []):
        val = it.get("ShippingAmount")
        if val in (None, ""):
            val = it.get("ShippingFeeAmount")
        try:
            envio += float(val or 0)
        except (TypeError, ValueError):
            pass
    if envio <= 0:
        for k in ("ShippingFeeTotal", "ShippingAmount"):
            try:
                v = float(order.get(k) or 0)
            except (TypeError, ValueError):
                v = 0.0
            if v > 0:
                envio = v
                break
    return round(envio, 2) if envio > 0 else 0.0


def fl_build_order_items(items: list, order: dict) -> list:
    """Convierte items de Falabella al formato interno order_items."""
    # Agrupar por nombre/SKU para consolidar cantidades
    grouped = {}
    for it in items:
        name  = str(it.get("Name") or it.get("SellerSku") or "Producto FL").strip()
        price = float(it.get("ItemPrice") or it.get("PaidPrice") or 0)
        qty   = float(it.get("Quantity") or it.get("QtyOrdered") or 1)
        key   = name
        if key in grouped:
            grouped[key]["quantity"] += qty
        else:
            grouped[key] = {"item": {"title": name}, "quantity": qty, "unit_price": price}
    return list(grouped.values())


def _fl_estados_reversion() -> set:
    """Estados de item Falabella que implican reversar el documento (NC).
    canceled/returned siempre; failed opcional via FL_NC_INCLUYE_FAILED."""
    rev = {"canceled", "cancelled", "returned"}
    if os.getenv("FL_NC_INCLUYE_FAILED", "").strip().lower() in ("1", "true", "on", "si", "yes"):
        rev = rev | {"failed"}
    return rev


def fl_detectar_reversion_total(order_id: str) -> Optional[str]:
    """Consulta los items de la orden y devuelve 'returned'/'canceled' SOLO si TODOS los items
    estan en un estado de reversion (devolucion/cancelacion total). Devuelve None si es parcial,
    mixto o no se pudo determinar (los parciales se registran para NC manual)."""
    try:
        items = fl_get_order_items(order_id)
    except Exception as e:
        logger.warning(f"[FL:{order_id}] no se pudo verificar reversion: {e}")
        return None
    if not items:
        return None
    rev = _fl_estados_reversion()
    estados = [str(it.get("Status") or "").strip().lower().replace(" ", "_") for it in items]
    if estados and all(e in rev for e in estados):
        return "returned" if "returned" in estados else "canceled"
    if any(e in rev for e in estados):
        logger.warning(f"[FL:{order_id}] Reversion PARCIAL (items: {estados}); requiere NC manual")
    return None


def auto_nota_credito_si_reversion_fl(venta: dict, tipo_rev: str):
    """Falabella: si la orden fue cancelada o devuelta TOTALMENTE y ya tenia documento emitido,
    crea la NC sola. Idempotente (no repite si la venta ya esta en 'nota_credito'). Nunca
    propaga la excepcion: si falla, la venta sigue igual y se registra en el log."""
    if not tipo_rev:
        return
    if NC_AUTO.get("falabella", {}).get("total") != "on":
        logger.info(f"[{(venta or {}).get('id')}] NC total automatica FL desactivada; queda para NC manual")
        return
    if not venta or not venta.get("move_id"):
        return
    if venta.get("estado") == "nota_credito":
        return  # ya tiene NC
    if venta.get("estado") != "enviado":
        return  # sin DTE emitido no hay nada que reversar
    motivo = ("Anulacion automatica Falabella (devolucion del comprador)"
              if tipo_rev == "returned" else
              "Anulacion automatica Falabella (orden cancelada)")
    try:
        nc_id = _crear_nota_credito(venta, motivo)
        logger.info(f"[{venta.get('id')}] NC automatica Falabella ({tipo_rev}): nc_move_id={nc_id}")
    except Exception as e:
        logger.error(f"[{venta.get('id')}] Error en NC automatica Falabella: {e}", exc_info=True)


def _fl_devueltos_de_items(items: list) -> dict:
    """{titulo: cantidad} de los items en estado de reversion (canceled/returned)."""
    rev = _fl_estados_reversion()
    out = {}
    for it in (items or []):
        st = str(it.get("Status") or "").strip().lower().replace(" ", "_")
        if st in rev:
            name = str(it.get("Name") or it.get("SellerSku") or "").strip()
            out[name] = out.get(name, 0) + float(it.get("Quantity") or it.get("QtyOrdered") or 1)
    return out


def auto_nota_credito_parcial_fl(venta: dict, items: list = None):
    """Falabella: devolucion PARCIAL (algunos items) con DTE emitido -> crea la NC parcial sola
    por los items devueltos (la factura sigue vigente). Gated por NC_AUTO[fl][parcial].
    Idempotente one-shot: no repite si ya se hizo una NC parcial (nc_motivo empieza con 'NC parcial')."""
    if NC_AUTO.get("falabella", {}).get("parcial") != "on":
        return
    if not venta or not venta.get("move_id") or venta.get("estado") != "enviado":
        return
    if (venta.get("nc_motivo") or "").lower().startswith("nc parcial"):
        return  # ya se hizo una NC parcial para esta venta
    oid = venta["id"]
    order_id = str(oid).replace("FL-", "", 1)
    if items is None:
        try:
            items = fl_get_order_items(order_id)
        except Exception as e:
            logger.warning(f"[{oid}] NC parcial auto: no se pudieron obtener items: {e}")
            return
    devueltos = _fl_devueltos_de_items(items)
    if not devueltos:
        return
    try:
        lineas = obtener_lineas_dte_odoo(venta["move_id"])
    except Exception as e:
        logger.warning(f"[{oid}] NC parcial auto: no se pudieron leer lineas DTE: {e}")
        return
    creditos = []
    for ln in lineas:
        for name, qty in devueltos.items():
            if name and (name == ln["name"] or name in ln["name"] or ln["name"] in name):
                cant = min(qty, ln["quantity"])
                if cant > 0:
                    creditos.append({"line_index": ln["line_index"], "cantidad": cant})
                break
    if not creditos:
        return
    try:
        nc_id = _crear_nota_credito_parcial(venta, creditos, "Devolucion parcial automatica Falabella")
        logger.info(f"[{oid}] NC PARCIAL automatica Falabella: nc_move_id={nc_id} items={creditos}")
    except Exception as e:
        logger.error(f"[{oid}] Error en NC parcial automatica Falabella: {e}", exc_info=True)


def fl_auto_nc(venta: dict):
    """Despachador de NC automatica FL para una venta emitida: con UNA consulta de items decide
    si la reversion es TOTAL (NC total) o PARCIAL (NC parcial). Respeta los toggles NC_AUTO."""
    if not venta or venta.get("estado") != "enviado" or not venta.get("move_id"):
        return
    if venta.get("estado") == "nota_credito":
        return
    oid = venta["id"]
    order_id = str(oid).replace("FL-", "", 1)
    try:
        items = fl_get_order_items(order_id)
    except Exception as e:
        logger.warning(f"[{oid}] fl_auto_nc: no se pudieron obtener items: {e}")
        return
    if not items:
        return
    rev = _fl_estados_reversion()
    estados = [str(it.get("Status") or "").strip().lower().replace(" ", "_") for it in items]
    if all(e in rev for e in estados):
        tipo = "returned" if "returned" in estados else "canceled"
        auto_nota_credito_si_reversion_fl(venta, tipo)
    elif any(e in rev for e in estados):
        auto_nota_credito_parcial_fl(venta, items)


def process_fl_order(order_id: str, order_data: dict = None):
    """Procesa una orden Falabella y la guarda en tabla ventas.
    Si order_data se provee (desde GetOrders), se usa directamente sin llamar GetOrder.
    """
    try:
        oid_str = f"FL-{order_id}"
        existing = get_venta(oid_str)

        datos_resumidos = False
        if order_data:
            order = order_data
            datos_resumidos = True
            logger.info(f"[FL:{order_id}] Usando datos de GetOrders (resumidos)")
        else:
            order = fl_get_order(order_id)
        if not order:
            logger.warning(f"[FL:{order_id}] Orden no encontrada")
            return

        # Normalizar status de forma robusta.
        # En GetOrders, Statuses suele venir como {"Status": ["delivered"]} (lista anidada);
        # tambien puede ser {"Status": "..."}, una lista, o un string directo.
        _s = order.get("Statuses")
        status_raw = None
        if isinstance(_s, dict):
            status_raw = _s.get("Status")
        elif isinstance(_s, (list, str)):
            status_raw = _s
        if isinstance(status_raw, list):
            status_raw = status_raw[0] if status_raw else None
        if isinstance(status_raw, dict):  # p.ej. {"Status": "..."} anidado
            status_raw = status_raw.get("Status")
        status_val = str(status_raw or "pending").strip().lower().replace(" ", "_")

        if existing:
            update_venta(oid_str, estado_envio=status_val)
            logger.info(f"[FL:{order_id}] Estado actualizado: {status_val}")
            # Si ya tenia DTE emitido, evaluar NC automatica (total o parcial) segun items.
            if existing.get("estado") == "enviado" and existing.get("move_id"):
                fl_auto_nc(existing)
            return

        if status_val not in FL_ESTADOS_VALIDOS:
            logger.info(f"[FL:{order_id}] Estado '{status_val}' no valido para facturar, ignorado "
                        f"(Statuses crudo: {order.get('Statuses')!r})")
            return

        extra_str = order.get("ExtraBillingAttributes") or ""
        billing   = fl_parse_extra_billing(extra_str)

        tipo      = fl_extract_tipo(order)
        rut       = fl_extract_rut(order, tipo, billing)
        nombre    = fl_extract_nombre(order, tipo, billing)
        giro      = fl_extract_giro(tipo, billing)
        email     = fl_extract_email(tipo, billing, order)
        direccion = fl_extract_direccion(tipo, billing, order)
        ciudad    = fl_extract_ciudad(tipo, billing, order)
        region    = fl_extract_region(tipo, billing, order)

        # Obtener items de la orden
        items_raw = []
        try:
            items_raw = fl_get_order_items(order_id)
        except Exception as e:
            logger.warning(f"[FL:{order_id}] No se pudo obtener items: {e}")

        # Si no hay items, usar el total de la orden como una linea generica
        if not items_raw:
            price_str = str(order.get("GrandTotal") or order.get("Price") or "0")
            price_clean = price_str.replace(",", "").replace(".", "").strip()
            try:
                price_val = float(price_clean)
            except ValueError:
                price_val = 0.0
            order_items = [{
                "item":       {"title": f"Orden Falabella #{order_id}"},
                "quantity":   1,
                "unit_price": price_val,
            }]
        else:
            order_items = fl_build_order_items(items_raw, order)

        telefono = fl_extract_telefono(order, billing)
        envio    = fl_extract_envio(items_raw, order)

        fake_order = {
            "id":          oid_str,
            "status":      status_val,
            "order_number": str(order.get("OrderNumber") or "").strip(),
            "order_items": order_items,
            "shipping_cost": envio,
            "telefono":    telefono,
            "buyer": {
                "email":      email,
                "phone":      telefono,
                "first_name": html_module.unescape(str(order.get("CustomerFirstName") or "")).strip(),
                "last_name":  html_module.unescape(str(order.get("CustomerLastName") or "")).strip(),
            },
        }

        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM ventas WHERE id = %s", (oid_str,))
                if cur.fetchone():
                    return
                cur.execute(
                    """
                    INSERT INTO ventas
                        (id, pack_id, cliente, rut, email, giro, direccion, ciudad, region,
                         tipo_sugerido, estado, estado_envio, order_json, billing_json, fuente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente', %s, %s, %s, 'falabella')
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        oid_str, None,
                        (nombre or "Cliente FL").strip(),
                        normalize_rut(rut) if rut else "",
                        (email or "").strip(),  # sin email real -> vacio, nunca el default
                        (giro or "").strip(),
                        (direccion or "").strip(),
                        (ciudad or "").strip(),
                        (region or "").strip(),
                        tipo, status_val,
                        json.dumps(fake_order, ensure_ascii=False),
                        json.dumps(billing, ensure_ascii=False),
                    ),
                )
            conn.commit()
        if datos_resumidos and not rut:
            logger.warning(f"[FL:{order_id}] Guardada con datos RESUMIDOS (GetOrders) sin RUT - reprocesar cuando whitelist disponible")
        else:
            logger.info(f"[FL:{order_id}] Guardada -> tipo={tipo} rut={rut or 'sin RUT'} cliente={nombre}")
            # Auto-emitir solo con datos completos (evita emitir con la linea generica de GetOrders)
            auto_emitir_venta(oid_str)

    except Exception as e:
        logger.error(f"[FL:{order_id}] Error procesando: {e}", exc_info=True)


def fl_reingesta_datos(oid: str) -> dict:
    """Reconsulta una orden Falabella y actualiza los datos guardados de la venta:
    recalcula envio + telefono + items y los graba en order_json (y campos cliente/rut/etc).
    - Si la venta NO esta emitida: al emitirla incluira envio + telefono.
    - Si YA esta emitida: el DTE no se puede modificar, pero se actualiza el telefono del
      partner en Odoo (los montos ya emitidos no se tocan)."""
    venta = get_venta(oid)
    if not venta:
        raise Exception("Venta no encontrada")
    order_id = oid.replace("FL-", "", 1)
    order = fl_get_order(order_id)
    if not order:
        raise Exception("Orden no encontrada en Falabella")

    extra_str = order.get("ExtraBillingAttributes") or ""
    billing   = fl_parse_extra_billing(extra_str)
    tipo      = venta.get("tipo_sugerido") or fl_extract_tipo(order)
    rut       = fl_extract_rut(order, tipo, billing) or venta.get("rut") or ""
    nombre    = fl_extract_nombre(order, tipo, billing) or venta.get("cliente") or "Cliente FL"
    giro      = fl_extract_giro(tipo, billing) or venta.get("giro") or ""
    _email_ext  = fl_extract_email(tipo, billing, order)
    _email_prev = (venta.get("email") or "").strip()
    if _email_prev.lower() in (FL_DEFAULT_EMAIL.lower(), ML_DEFAULT_EMAIL.lower()):
        _email_prev = ""  # descartar defaults guardados antes
    email     = _email_ext or _email_prev  # puede quedar "" (sin email real)
    direccion = fl_extract_direccion(tipo, billing, order) or venta.get("direccion") or ""
    ciudad    = fl_extract_ciudad(tipo, billing, order) or venta.get("ciudad") or ""
    region    = fl_extract_region(tipo, billing, order) or venta.get("region") or ""
    telefono  = fl_extract_telefono(order, billing)

    items_raw = []
    try:
        items_raw = fl_get_order_items(order_id)
    except Exception as e:
        logger.warning(f"[{oid}] reingesta: no se pudo obtener items: {e}")
    if items_raw:
        order_items = fl_build_order_items(items_raw, order)
    else:
        prev = json.loads(venta.get("order_json") or "{}")
        order_items = prev.get("order_items") or []
    envio = fl_extract_envio(items_raw, order)

    fake_order = {
        "id":            oid,
        "status":        venta.get("estado_envio") or "",
        "order_number":  str(order.get("OrderNumber") or "").strip(),
        "order_items":   order_items,
        "shipping_cost": envio,
        "telefono":      telefono,
        "buyer": {
            "email":      email,
            "phone":      telefono,
            "first_name": html_module.unescape(str(order.get("CustomerFirstName") or "")).strip(),
            "last_name":  html_module.unescape(str(order.get("CustomerLastName") or "")).strip(),
        },
    }
    update_venta(
        oid,
        cliente=(nombre or "Cliente FL").strip(),
        rut=normalize_rut(rut) if rut else "",
        email=(email or "").strip(),  # vacio si no hay email real del cliente
        giro=(giro or "").strip(),
        direccion=(direccion or "").strip(),
        ciudad=(ciudad or "").strip(),
        region=(region or "").strip(),
        order_json=json.dumps(fake_order, ensure_ascii=False),
        billing_json=json.dumps(billing, ensure_ascii=False),
    )

    tel_partner = False
    if telefono and venta.get("partner_id"):
        try:
            ctx = odoo_connect()
            pf = get_partner_fields(ctx)
            pv = {}
            if "phone" in pf:
                pv["phone"] = telefono
            if "mobile" in pf and re.sub(r"\D", "", telefono).lstrip("56").startswith("9"):
                pv["mobile"] = telefono
            if pv:
                odoo_exec(ctx, "res.partner", "write", [[venta["partner_id"]], pv])
                tel_partner = True
        except Exception as e:
            logger.warning(f"[{oid}] reingesta: no se pudo actualizar telefono del partner: {e}")

    logger.info(f"[{oid}] Reingesta FL: telefono={telefono or '-'} envio={envio} "
                f"emitida={venta.get('estado') == 'enviado'} tel_partner={tel_partner}")
    return {
        "id": oid, "telefono": telefono, "envio": envio,
        "emitida": venta.get("estado") == "enviado", "telefono_partner_actualizado": tel_partner,
    }


def fl_webhook_worker():
    while True:
        try:
            order_id = fl_webhook_queue.get(timeout=5)
            try:
                process_fl_order(str(order_id))
                time.sleep(2)
            except Exception as e:
                logger.error(f"[FL:{order_id}] Error en worker: {e}")
                time.sleep(5)
            finally:
                fl_webhook_queue.task_done()
        except queue_module.Empty:
            continue


def reconciliar_fl_ordenes():
    """Reconciliacion AUTOMATICA de Falabella: cada FL_RECON_INTERVAL_MIN (default 15 min) revisa
    las ordenes recientes e ingesta las faltantes (hasta FL_RECON_MAX por ciclo). Es el respaldo
    del webhook /fl/webhook (que da tiempo real si esta configurado en Seller Center)."""
    _intervalo = int(os.getenv("FL_RECON_INTERVAL_MIN", "15")) * 60
    while True:
        time.sleep(_intervalo)
        try:
            ordenes = fl_get_orders_recent(limit=100)
            if not ordenes:
                continue
            ids_fl = [f"FL-{o['OrderId']}" for o in ordenes if o.get("OrderId")]
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM ventas WHERE id = ANY(%s::text[])", (ids_fl,))
                    ids_en_bd = {str(row["id"]) for row in cur.fetchall()}
            def fl_get_status(o):
                """Extrae status de forma robusta sin importar el shape de Statuses."""
                s = o.get("Statuses")
                if isinstance(s, dict):
                    val = s.get("Status", "")
                elif isinstance(s, list):
                    val = s[0] if s else ""
                elif isinstance(s, str):
                    val = s
                else:
                    val = ""
                return str(val).strip().lower().replace(" ", "_")

            faltantes = [o for o in ordenes if f"FL-{o['OrderId']}" not in ids_en_bd
                         and fl_get_status(o) in FL_ESTADOS_VALIDOS]
            _max_recon = int(os.getenv("FL_RECON_MAX", "50"))
            if faltantes:
                logger.warning(f"[FL] Reconciliacion: {len(faltantes)} faltantes, procesando hasta {_max_recon}")
                for o in faltantes[:_max_recon]:
                    try:
                        # Pasar order_data directamente desde GetOrders (evita llamar GetOrder)
                        process_fl_order(str(o["OrderId"]), order_data=o)
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"[FL] Error procesando {o['OrderId']}: {e}")
                        time.sleep(2)
            else:
                logger.info(f"[FL] Reconciliacion OK: {len(ordenes)} ordenes en BD")

            # Cancelaciones / devoluciones en ordenes YA emitidas: crear NC automatica.
            rev = _fl_estados_reversion()
            def _statuses_tokens(o):
                s = o.get("Statuses")
                vals = s.get("Status") if isinstance(s, dict) else s
                if not isinstance(vals, list):
                    vals = [vals]
                return {str(v).strip().lower().replace(" ", "_") for v in vals if v}
            for o in ordenes:
                oid_o = f"FL-{o['OrderId']}" if o.get("OrderId") else None
                if not oid_o or oid_o not in ids_en_bd:
                    continue
                if not (_statuses_tokens(o) & rev):
                    continue
                v = get_venta(oid_o)
                if not v or v.get("estado") != "enviado" or not v.get("move_id"):
                    continue
                fl_auto_nc(v)  # decide total o parcial con una sola consulta de items
                time.sleep(1)
        except Exception as e:
            logger.error(f"[FL] Error en reconciliacion: {e}")


def verify_fl_webhook(body_bytes: bytes, signature: str) -> bool:
    """Verifica firma HMAC del webhook de Falabella."""
    secret = get_env("FL_WEBHOOK_SECRET", required=False, default="")
    if not secret:
        logger.warning("[FL] FL_WEBHOOK_SECRET no configurado, omitiendo verificacion")
        return True
    mac = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256)
    expected = base64.b64encode(mac.digest()).decode("utf-8")
    return hmac.compare_digest(expected, signature)

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
    .card.activa{border-color:#3b82f6;background:#0f1f3d;}
    .card#cardML.activa{border-color:#3b82f6;background:#0a1628;}
    .card#cardWC.activa{border-color:#22c55e;background:#052e16;}
    .card#cardFL.activa{border-color:#f59e0b;background:#2a1a00;}
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
    .badge-nc{background:rgba(251,191,36,0.15);color:#fbbf24;}
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
      <div class="title">&#x1F6D2; Bandeja de ventas ML + WooCommerce &rarr; Odoo</div>
      <div class="subtitle">Revisa, edita, reprocesa y autoriza cada venta antes de crear el documento en Odoo.</div>
    </div>
    <div class="actions">
      <button class="secondary" onclick="refreshData()">&#8635; Actualizar</button>
      <a class="link" href="/health" target="_blank">Health</a>
      <a class="link" href="/ventas" target="_blank">API</a>
    </div>
  </div>
  <div id="cafAlert"></div>
  <div class="grid" style="grid-template-columns:repeat(5,1fr)">
    <div class="card" id="cardTodas" style="cursor:pointer" onclick="setFuente('')">
      <h3>Total</h3><div class="value" id="cTotal">&mdash;</div>
      <div style="font-size:12px;color:var(--muted);margin-top:4px">Todas las fuentes</div>
    </div>
    <div class="card" id="cardML" style="cursor:pointer" onclick="setFuente('mercadolibre')">
      <h3>&#x1F6CD; Mercado Libre</h3><div class="value" id="cML">&mdash;</div>
      <div style="font-size:12px;color:#93c5fd;margin-top:4px" id="cMLpend"></div>
    </div>
    <div class="card" id="cardWC" style="cursor:pointer" onclick="setFuente('woocommerce')">
      <h3>&#x1F6D2; WooCommerce</h3><div class="value" id="cWC">&mdash;</div>
      <div style="font-size:12px;color:#86efac;margin-top:4px" id="cWCpend"></div>
    </div>
    <div class="card" id="cardFL" style="cursor:pointer" onclick="setFuente('falabella')">
      <h3>&#x1F7E1; Falabella</h3><div class="value" id="cFL">&mdash;</div>
      <div style="font-size:12px;color:#fbbf24;margin-top:4px" id="cFLpend"></div>
    </div>
    <div class="card">
      <h3>Pendientes / Error</h3><div class="value" id="cPend">&mdash;</div>
      <div style="font-size:12px;color:#f87171;margin-top:4px" id="cErr"></div>
    </div>
  </div>
  <div style="margin-bottom:12px;padding:12px 14px;background:var(--panel);border:1px solid var(--border);border-radius:12px">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">&#9889; Auto-emisi&oacute;n autom&aacute;tica por canal
      <span id="cfgEstado" style="font-weight:400;font-size:12px;color:var(--muted);margin-left:8px"></span></div>
    <div style="display:grid;grid-template-columns:auto auto auto;gap:8px 20px;align-items:center">
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Canal</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Boletas</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Facturas</span>

      <span style="font-size:13px">&#x1F6CD; Mercado Libre</span>
      <select id="cfgMLBoletas" onchange="guardarAutoEmision('mercadolibre')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>
      <select id="cfgMLFacturas" onchange="guardarAutoEmision('mercadolibre')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>

      <span style="font-size:13px">&#x1F6D2; WooCommerce</span>
      <select id="cfgWCBoletas" onchange="guardarAutoEmision('woocommerce')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>
      <select id="cfgWCFacturas" onchange="guardarAutoEmision('woocommerce')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>

      <span style="font-size:13px">&#x1F7E1; Falabella</span>
      <select id="cfgFLBoletas" onchange="guardarAutoEmision('falabella')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>
      <select id="cfgFLFacturas" onchange="guardarAutoEmision('falabella')" style="padding:6px 10px"><option value="auto">Autom&aacute;tica</option><option value="manual">Manual</option></select>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">Facturas en "Autom&aacute;tica" se emiten solo si tienen raz&oacute;n social + RUT + direcci&oacute;n + giro; si no, quedan pendientes.</div>
  </div>
  <div style="margin-bottom:12px;padding:12px 14px;background:var(--panel);border:1px solid var(--border);border-radius:12px">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">&#9989; Acciones post-emisi&oacute;n por canal
      <span id="cfgPostEstado" style="font-weight:400;font-size:12px;color:var(--muted);margin-left:8px"></span></div>
    <div style="display:grid;grid-template-columns:auto auto auto auto;gap:8px 20px;align-items:center">
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Canal</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Marcar pagada (Odoo)</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Enviar email al cliente</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Cargar documento tributario</span>

      <span style="font-size:13px">&#x1F6CD; Mercado Libre</span>
      <select id="peMLpagar" onchange="guardarPostEmision('mercadolibre')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <span style="font-size:11px;color:#64748b">No aplica (sin email real)</span>
      <select id="peMLadjuntar" onchange="guardarPostEmision('mercadolibre')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>

      <span style="font-size:13px">&#x1F6D2; WooCommerce</span>
      <select id="peWCpagar" onchange="guardarPostEmision('woocommerce')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="peWCemail" onchange="guardarPostEmision('woocommerce')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <span style="font-size:11px;color:#64748b">No aplica</span>

      <span style="font-size:13px">&#x1F7E1; Falabella</span>
      <select id="peFLpagar" onchange="guardarPostEmision('falabella')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="peFLemail" onchange="guardarPostEmision('falabella')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="peFLadjuntar" onchange="guardarPostEmision('falabella')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">"Marcar pagada" registra el pago en Odoo por el diario de banco (queda PAGADA). "Enviar email" manda el comprobante al cliente por el correo interno de Odoo (WooCommerce y Falabella). "Cargar documento tributario" sube el PDF del DTE al marketplace: en ML al pack (packs/fiscal_documents, no aplica a env&iacute;os Full); en Falabella v&iacute;a SetInvoicePDF (solo &oacute;rdenes en ready_to_ship o posterior; no aplica a env&iacute;os por Falabella/FBF).</div>
  </div>
  <div style="margin-bottom:12px;padding:12px 14px;background:var(--panel);border:1px solid var(--border);border-radius:12px">
    <div style="font-size:13px;font-weight:700;margin-bottom:10px">&#128203; Nota de Cr&eacute;dito autom&aacute;tica por canal
      <span id="cfgNcEstado" style="font-weight:400;font-size:12px;color:var(--muted);margin-left:8px"></span></div>
    <div style="display:grid;grid-template-columns:auto auto auto;gap:8px 20px;align-items:center">
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">Canal</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">NC total (anula todo)</span>
      <span style="color:var(--muted);font-size:11px;text-transform:uppercase">NC parcial (solo devuelto)</span>

      <span style="font-size:13px">&#x1F6CD; Mercado Libre</span>
      <select id="ncMLtotal" onchange="guardarNcAuto('mercadolibre')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="ncMLparcial" onchange="guardarNcAuto('mercadolibre')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>

      <span style="font-size:13px">&#x1F6D2; WooCommerce</span>
      <select id="ncWCtotal" onchange="guardarNcAuto('woocommerce')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="ncWCparcial" onchange="guardarNcAuto('woocommerce')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>

      <span style="font-size:13px">&#x1F7E1; Falabella</span>
      <select id="ncFLtotal" onchange="guardarNcAuto('falabella')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
      <select id="ncFLparcial" onchange="guardarNcAuto('falabella')" style="padding:6px 10px"><option value="off">No</option><option value="on">S&iacute;</option></select>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">"NC total" anula la factura completa cuando la orden se cancela o devuelve ENTERA. "NC parcial" acredita solo los items devueltos (la factura sigue vigente) cuando la devoluci&oacute;n es parcial. La detecci&oacute;n autom&aacute;tica de parciales opera hoy en Falabella (por estado de cada item); en ML las parciales quedan manuales. Con todo en "No", las NC se hacen a mano con el bot&oacute;n N/C.</div>
  </div>
  <div class="toolbar">
    <input id="searchInput" placeholder="Buscar por ID, cliente, RUT, email..." oninput="resetYRender()">
    <select id="statusFilter" onchange="resetYRender()">
      <option value="pendiente">Pendiente</option>
      <option value="">Todos los estados</option>
      <option value="enviado">Enviado</option>
      <option value="error">Error</option>
      <option value="rechazado">Rechazado</option>
      <option value="nota_credito">Nota de credito</option>
      <option value="dividida">Dividida</option>
    </select>
    <select id="pageSize" onchange="resetYRender()" title="Filas por pagina">
      <option value="50">50 por pagina</option>
      <option value="100">100 por pagina</option>
      <option value="500">500 por pagina</option>
      <option value="1000">1000 por pagina</option>
    </select>
    <input type="hidden" id="fuenteFilter" value="">
    <select id="horaCorte" onchange="resetYRender()">
      <option value="14">Corte 14:00</option>
      <option value="15">Corte 15:00</option>
      <option value="13">Corte 13:00</option>
      <option value="12">Corte 12:00</option>
    </select>
    <button class="secondary" onclick="abrirCalendario()" id="btnCalendario">&#128197; Todos los turnos</button>
    <button class="success" onclick="abrirCrearCliente()">+ Crear cliente</button>
    <button class="secondary" onclick="abrirIngresarVenta()" style="background:var(--blue)">+ Ingresar venta</button>
    <button class="warn" onclick="reprocesarTodo()">&#8635; Reprocesar todo</button>
    <button class="secondary" onclick="reconciliarML()" title="Consulta las ultimas 200 ordenes en ML">&#128279; Reconciliar ML</button>
    <button class="bad" onclick="revisarCanceladasML()" title="Crea NC de ventas ML ya facturadas que se cancelaron o dividieron despues">&#8617; Canceladas/NC ML</button>
    <button class="secondary" onclick="reconciliarWC()" title="Consulta las ultimas 100 ordenes en WooCommerce">&#128666; Reconciliar WC</button>
    <button class="secondary" onclick="reconciliarFL()" title="Consulta las ordenes recientes en Falabella (te pregunta cuantos dias) e ingresa las que falten">&#127873; Reconciliar FL</button>
    <button class="secondary" onclick="ingresarFL()" title="Ingresa una orden de Falabella por su OrderId (para pruebas)">&#128229; Ingresar orden FL</button>
    <button class="warn" onclick="reprocesarDatosFL()" title="Reconsulta las ordenes de Falabella y actualiza envio + telefono en las ventas existentes">&#128260; Reprocesar datos FL</button>
    <button class="bad" onclick="revisarDevolucionesFL()" title="Crea NC en las ventas Falabella emitidas cuya orden fue cancelada o devuelta">&#8617; Devoluciones/Cancelaciones FL</button>
    <button class="secondary" onclick="actualizarEnvio()" title="Actualiza el tipo de envio en ventas ML">&#128666; Actualizar envios</button>
    <button class="warn" onclick="recalcularWCTodos()" title="Corrige montos (IVA + envio) de ventas WooCommerce pendientes">&#128260; Recalcular WC</button>
    <button id="btnAgrupar" class="pack-btn" style="display:none" onclick="agruparSeleccionadas()">&#9935; Agrupar seleccionadas</button>
    <button id="btnAutorizarMasivo" class="success" style="display:none" onclick="autorizarMasivo()">&#10003; Autorizar seleccionadas</button>
  </div>
  <div id="selInfo" style="display:none;margin-bottom:10px;font-size:13px;color:var(--muted)"><span id="selCount"></span></div>
  <div id="tableWrap"><div class="empty">Cargando...</div></div>
  <div id="pager"></div>
</div>

<!-- Modal Calendario -->
<div class="modal" id="calModal">
  <div class="modal-card" style="width:min(1050px,96vw)">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div class="title" style="font-size:20px">&#128197; Calendario de ventas</div>
      <button class="secondary" onclick="cerrarCalendario()">Cerrar</button>
    </div>
    <div style="margin-bottom:14px;display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
      <div style="display:flex;align-items:center;gap:8px;">
        <button class="secondary" onclick="cambiarAnio(-1)">&#9664;</button>
        <span id="calYearLabel" style="font-size:18px;font-weight:700;min-width:150px;text-align:center"></span>
        <button class="secondary" onclick="cambiarAnio(1)">&#9654;</button>
      </div>
      <div style="display:flex;align-items:center;gap:8px;">
        <span style="font-size:13px;color:var(--muted)">Hora de corte:</span>
        <select id="horaCorte2" onchange="sincronizarCorte(this.value); renderCalendario()">
          <option value="14">14:00</option>
          <option value="15">15:00</option>
          <option value="13">13:00</option>
          <option value="12">12:00</option>
        </select>
      </div>
      <button class="secondary" onclick="seleccionarTurno(''); cerrarCalendario()">Ver todas las ventas</button>
    </div>
    <div id="calGrid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px;"></div>
  </div>
</div>

<!-- Modal NC -->
<div class="modal" id="ncModal">
  <div class="modal-card" style="max-width:500px">
    <div class="topbar" style="margin-bottom:0">
      <div><div class="title" style="font-size:20px">&#128203; Nota de Credito</div>
      <div class="subtitle" id="ncModalSub"></div></div>
      <button class="secondary" onclick="cerrarNC()">Cerrar</button>
    </div>
    <div style="margin-top:16px;padding:12px;background:var(--panel2);border-radius:8px;font-size:13px" id="ncModalInfo"></div>
    <div style="margin-top:14px;display:flex;align-items:center;gap:8px">
      <input type="checkbox" id="ncParcial" onchange="toggleNcParcial()" style="width:auto">
      <label for="ncParcial" style="margin:0;cursor:pointer">NC parcial (acreditar solo algunos items / cantidades). La factura original queda vigente.</label>
    </div>
    <div id="ncLineasBox" style="margin-top:12px;display:none">
      <div style="font-size:12px;color:var(--muted);margin-bottom:6px">Indica la cantidad a acreditar por item (0 = no se acredita). Los devueltos del marketplace vienen sugeridos.</div>
      <div id="ncLineas" style="max-height:240px;overflow:auto;border:1px solid var(--border);border-radius:8px"></div>
    </div>
    <div style="margin-top:16px">
      <label>Motivo de la nota de credito</label>
      <select id="ncMotivo" onchange="toggleNcOtro()">
        <option value="">-- Seleccionar motivo --</option>
        <option value="Devuelto">Devuelto</option>
        <option value="Error en la entrega">Error en la entrega</option>
        <option value="Producto llego en mal estado">Producto llego en mal estado</option>
        <option value="Producto enviado distinto al solicitado">Producto enviado distinto al solicitado</option>
        <option value="No despachado">No despachado</option>
        <option value="otro">Otro motivo...</option>
      </select>
    </div>
    <div style="margin-top:12px;display:none" id="ncOtroGroup">
      <label>Especificar motivo</label>
      <input id="ncOtro" placeholder="Describe el motivo">
    </div>
    <div id="ncError" style="color:#f87171;font-size:13px;margin-top:12px;display:none"></div>
    <div class="modal-actions">
      <button class="secondary" onclick="cerrarNC()">Cancelar</button>
      <button class="bad" onclick="confirmarNC()">Crear Nota de Credito</button>
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
      <div class="full"><label>Tipo</label>
        <select id="cliTipo" onchange="toggleCliGiro()">
          <option value="persona">Persona natural (Boleta)</option>
          <option value="empresa">Empresa (Factura)</option>
        </select></div>
      <div class="full"><label>Nombre / Razon social</label><input id="cliNombre" placeholder="Nombre completo o razon social"></div>
      <div><label>RUT</label><input id="cliRut" placeholder="12345678-9"></div>
      <div><label>Email DTE</label><input id="cliEmail" placeholder="correo@ejemplo.com"></div>
      <div class="full"><label>Direccion</label><input id="cliDireccion" placeholder="Calle y numero"></div>
      <div><label>Ciudad / Comuna</label><input id="cliCiudad" placeholder="Las Condes"></div>
      <div><label>Region</label><select id="cliRegion">
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
      <div class="full" id="cliGiroGroup" style="display:none"><label>Giro / Actividad economica</label><input id="cliGiro" placeholder="Comercio al por menor"></div>
    </div>
    <div id="cliError" style="color:#f87171;font-size:13px;margin-top:12px;display:none"></div>
    <div class="modal-actions">
      <button class="secondary" onclick="cerrarCrearCliente()">Cancelar</button>
      <button class="success" onclick="crearClienteOdoo()">Crear en Odoo</button>
    </div>
  </div>
</div>

<!-- Modal Editar -->
<div class="modal" id="editModal">
  <div class="modal-card">
    <div class="topbar" style="margin-bottom:0">
      <div><div class="title" style="font-size:20px">&#9999;&#65039; Editar venta</div><div class="subtitle" id="modalSub"></div></div>
      <button class="secondary" onclick="closeModal()">Cerrar</button>
    </div>
    <div class="modal-grid">
      <div><label>ID venta</label><input id="editId" disabled></div>
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
      <div><label>Cambiar estado (manual, no toca Odoo)</label>
        <select id="editEstado">
          <option value="">-- mantener --</option>
          <option value="pendiente">pendiente</option>
          <option value="enviado">enviado</option>
          <option value="error">error</option>
          <option value="rechazado">rechazado</option>
          <option value="nota_credito">nota_credito</option>
        </select></div>
      <div><label>Total bruto</label><input id="editTotal" disabled></div>
      <div><label>Cantidad items</label><input id="editItemsCount" disabled></div>
      <div class="full"><label>Productos vendidos</label><textarea id="editProducts" disabled></textarea></div>
    </div>
    <div class="modal-actions">
      <button class="secondary" onclick="closeModal()">Cancelar</button>
      <button class="warn" onclick="reprocesarActual()">Reprocesar desde ML</button>
      <button class="secondary" onclick="cambiarEstadoActual()">Aplicar estado</button>
      <button onclick="saveEdit()">Guardar</button>
      <button class="success" onclick="saveAndAuthorize()">Guardar y autorizar</button>
    </div>
  </div>
</div>

<!-- Modal Pack -->
<div class="modal" id="packModal">
  <div class="modal-card" style="max-width:680px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div><div class="title" style="font-size:20px">Pack</div><div class="subtitle" id="packModalTitle"></div></div>
      <button class="secondary" onclick="closePackModal()">Cerrar</button>
    </div>
    <div id="packModalBody"></div>
  </div>
</div>

<!-- Modal Venta Manual -->
<div class="modal" id="ventaManualModal">
  <div class="modal-card" style="max-width:700px">
    <div class="topbar" style="margin-bottom:0">
      <div><div class="title" style="font-size:20px">+ Ingresar venta manual</div>
      <div class="subtitle">Crea una venta directamente sin pasar por ML o WooCommerce</div></div>
      <button class="secondary" onclick="cerrarIngresarVenta()">Cerrar</button>
    </div>
    <div class="modal-grid" style="margin-top:16px">
      <div><label>Tipo documento</label><select id="vmTipo" onchange="toggleVmGiro()"><option value="Boleta">Boleta</option><option value="Factura">Factura</option></select></div>
      <div><label>ID orden (opcional)</label><input id="vmOrderId" placeholder="ej: 2000012419074761"></div>
      <div class="full"><label>Nombre / Razon social</label><input id="vmCliente" placeholder="Nombre completo o razon social"></div>
      <div><label>RUT</label><input id="vmRut" placeholder="12345678-9"></div>
      <div><label>Email DTE</label><input id="vmEmail" placeholder="correo@ejemplo.com" value="boleta@lemulux.com"></div>
      <div class="full"><label>Direccion</label><input id="vmDireccion" placeholder="Calle y numero"></div>
      <div><label>Ciudad / Comuna</label><input id="vmCiudad" placeholder="Las Condes"></div>
      <div><label>Region</label><select id="vmRegion">
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
      <div class="full" id="vmGiroGroup" style="display:none"><label>Giro / Actividad economica</label><input id="vmGiro" placeholder="Comercio al por menor"></div>
      <div class="full"><label>Productos (uno por linea)</label><textarea id="vmProductos" style="min-height:120px" placeholder="Foco Led 24w x2&#10;Tubo Led T8 x1"></textarea></div>
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
var currentPage = 1;
var calYear = null;

function badge(estado) {
  var map = {pendiente:'badge-pendiente', enviado:'badge-enviado', error:'badge-error', rechazado:'badge-default', nota_credito:'badge-nc', dividida:'badge-default'};
  var label = {nota_credito:'N/C', dividida:'Dividida'};
  var extra = estado === 'dividida' ? ' style="background:#3b2a12;color:#fbbf24"' : '';
  return '<span class="badge ' + (map[estado] || 'badge-default') + '"' + extra + '>' + esc(label[estado] || estado) + '</span>';
}

function fuentebadge(fuente) {
  if (!fuente || fuente === 'mercadolibre') {
    return '<span style="background:#1a2744;color:#93c5fd;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:700;margin-left:4px">ML</span>';
  }
  if (fuente === 'woocommerce') {
    return '<span style="background:#1a3a2a;color:#86efac;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:700;margin-left:4px">WC</span>';
  }
  if (fuente === 'falabella') {
    return '<span style="background:#1a2010;color:#fbbf24;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:700;margin-left:4px">FL</span>';
  }
  return '<span style="background:#2a1a3a;color:#c4b5fd;padding:2px 7px;border-radius:999px;font-size:11px;font-weight:700;margin-left:4px">MAN</span>';
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
  resetYRender();
}

function setFuente(f) {
  document.getElementById('fuenteFilter').value = f;
  ['cardTodas','cardML','cardWC','cardFL'].forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.classList.remove('activa');
  });
  if (f === '') document.getElementById('cardTodas').classList.add('activa');
  else if (f === 'mercadolibre') document.getElementById('cardML').classList.add('activa');
  else if (f === 'woocommerce') document.getElementById('cardWC').classList.add('activa');
  else if (f === 'falabella') { var cfl = document.getElementById('cardFL'); if (cfl) cfl.classList.add('activa'); }
  resetYRender();
}

function updateStats(items) {
  var totalML  = ventas.filter(function(v){ return (v.fuente || 'mercadolibre') === 'mercadolibre'; }).length;
  var totalWC  = ventas.filter(function(v){ return v.fuente === 'woocommerce'; }).length;
  var totalFL  = ventas.filter(function(v){ return v.fuente === 'falabella'; }).length;
  var pendML   = ventas.filter(function(v){ return (v.fuente || 'mercadolibre') === 'mercadolibre' && v.estado === 'pendiente'; }).length;
  var pendWC   = ventas.filter(function(v){ return v.fuente === 'woocommerce' && v.estado === 'pendiente'; }).length;
  var pendFL   = ventas.filter(function(v){ return v.fuente === 'falabella' && v.estado === 'pendiente'; }).length;
  var errML    = ventas.filter(function(v){ return (v.fuente || 'mercadolibre') === 'mercadolibre' && v.estado === 'error'; }).length;
  var errWC    = ventas.filter(function(v){ return v.fuente === 'woocommerce' && v.estado === 'error'; }).length;
  var errFL    = ventas.filter(function(v){ return v.fuente === 'falabella' && v.estado === 'error'; }).length;
  var totalPend = ventas.filter(function(v){ return v.estado === 'pendiente'; }).length;
  var totalErr  = ventas.filter(function(v){ return v.estado === 'error'; }).length;
  document.getElementById('cTotal').textContent = ventas.length;
  document.getElementById('cML').textContent = totalML;
  document.getElementById('cWC').textContent = totalWC;
  document.getElementById('cFL').textContent = totalFL;
  document.getElementById('cPend').textContent = totalPend;
  document.getElementById('cMLpend').textContent = pendML + ' pend' + (errML ? ' / ' + errML + ' err' : '');
  document.getElementById('cWCpend').textContent = pendWC + ' pend' + (errWC ? ' / ' + errWC + ' err' : '');
  document.getElementById('cFLpend').textContent = pendFL + ' pend' + (errFL ? ' / ' + errFL + ' err' : '');
  document.getElementById('cErr').textContent = totalErr > 0 ? totalErr + ' con error' : '';
}

function filteredVentas() {
  var q = document.getElementById('searchInput').value.trim().toLowerCase();
  var s = document.getElementById('statusFilter').value;
  var f = (document.getElementById('fuenteFilter') || {value:''}).value;
  return ventas.filter(function(v) {
    var okS = !s || v.estado === s;
    var okF = !f || (v.fuente || 'mercadolibre') === f;
    var campos = [v.id, v.order_number, v.cliente, v.rut, v.email, v.tipo_sugerido, v.direccion, v.giro].filter(Boolean).join(' ').toLowerCase();
    var okQ = !q || campos.indexOf(q) >= 0;
    var okT = !turnoActivo || getTurnoKey(v.creado_en) === turnoActivo;
    return okS && okQ && okT && okF;
  });
}

function rowHtml(v) {
  var id = String(v.id || '');
  var fecha = v.creado_en ? new Date(v.creado_en + 'Z').toLocaleString('es-CL', {timeZone:'America/Santiago'}) : '-';
  var acciones = '';
  acciones += '<button class="secondary" data-action="edit" data-id="' + esc(id) + '">Editar</button> ';
  if ((v.fuente || 'mercadolibre') === 'mercadolibre') {
    acciones += '<button class="warn" data-action="reprocesar" data-id="' + esc(id) + '">Reprocesar</button> ';
  }
  if (v.fuente === 'woocommerce' && v.estado !== 'enviado') {
    acciones += '<button class="warn" data-action="recalcwc" data-id="' + esc(id) + '">Recalcular</button> ';
  }
  if (v.pack_id) {
    acciones += '<button class="pack-btn" data-action="verpack" data-id="' + esc(id) + '" data-pack="' + esc(v.pack_id) + '">Pack</button> ';
  }
  if (v.estado !== 'enviado') {
    acciones += '<button class="success" data-action="autorizar" data-id="' + esc(id) + '">Autorizar</button>';
  }
  if (v.estado === 'enviado') {
    acciones += '<button class="bad" data-action="anular" data-id="' + esc(id) + '">Anular</button>';
    acciones += '<button class="bad" style="background:#92400e;border-color:#92400e" data-action="notacredito" data-id="' + esc(id) + '">N/C</button>';
  }
  if ((v.fuente || 'mercadolibre') === 'mercadolibre' && (v.estado === 'pendiente' || v.estado === 'enviado')) {
    acciones += '<button data-action="adjuntarml" data-id="' + esc(id) + '" title="Emite (si falta) y sube el PDF del comprobante a Mercado Libre" style="background:#0ea5e9;color:#fff;border:none;border-radius:8px;padding:9px 12px;font-weight:600;cursor:pointer">Cargar PDF a ML</button>';
  }
  if (v.fuente === 'falabella' && (v.estado === 'pendiente' || v.estado === 'enviado')) {
    acciones += '<button data-action="adjuntarfl" data-id="' + esc(id) + '" title="Emite (si falta) y sube el PDF del documento tributario a Falabella" style="background:#16a34a;color:#fff;border:none;border-radius:8px;padding:9px 12px;font-weight:600;cursor:pointer">Cargar PDF a FL</button>';
  }
  return '<tr id="row-' + esc(id) + '">' +
    '<td><input type="checkbox" class="cb-row" data-id="' + esc(id) + '" onchange="onCheckboxChange()"></td>' +
    '<td>' + safe(fecha) + '<div class="small">' + safe(id) + '</div>' +
      (v.order_number ? '<div class="small" style="color:#fbbf24">N&deg; ' + safe(v.order_number) + '</div>' : '') +
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
    '<td>' + badge(v.estado) + fuentebadge(v.fuente) +
      (v.error ? '<div class="small" style="color:#f87171;margin-top:4px">' + safe(v.error).substring(0,80) + '</div>' : '') + '</td>' +
    '<td><span>' + safe(v.estado_envio || 'paid') + '</span></td>' +
    '<td><div class="row-actions">' + acciones + '</div></td>' +
    '</tr>';
}

function resetYRender() { currentPage = 1; renderTable(); }

function irPagina(p) {
  currentPage = p;
  renderTable();
  try { document.getElementById('tableWrap').scrollIntoView({block: 'start'}); } catch (e) {}
}

function renderPager(total, totalPages, start, shown) {
  var pager = document.getElementById('pager');
  if (!pager) return;
  if (!total) { pager.innerHTML = ''; return; }
  var from = start + 1, to = start + shown;
  var nav = '';
  nav += '<button class="secondary" ' + (currentPage <= 1 ? 'disabled' : '') + ' onclick="irPagina(1)">&#171; Primera</button>';
  nav += '<button class="secondary" ' + (currentPage <= 1 ? 'disabled' : '') + ' onclick="irPagina(' + (currentPage - 1) + ')">&#8249; Anterior</button>';
  nav += '<span style="font-size:13px;padding:0 6px">P&aacute;gina ' + currentPage + ' de ' + totalPages + '</span>';
  nav += '<button class="secondary" ' + (currentPage >= totalPages ? 'disabled' : '') + ' onclick="irPagina(' + (currentPage + 1) + ')">Siguiente &#8250;</button>';
  nav += '<button class="secondary" ' + (currentPage >= totalPages ? 'disabled' : '') + ' onclick="irPagina(' + totalPages + ')">&#218;ltima &#187;</button>';
  pager.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-top:12px">' +
    '<span style="font-size:13px;color:var(--muted)">Mostrando ' + from + '&ndash;' + to + ' de ' + total + '</span>' +
    '<div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">' + nav + '</div></div>';
}

function cambiarAnio(delta) { if (calYear == null) return; calYear += delta; renderCalendario(); }

function renderTable() {
  var items = filteredVentas();
  updateStats(items);
  var wrap = document.getElementById('tableWrap');
  var pageSize = parseInt((document.getElementById('pageSize') || {value: '50'}).value || '50', 10);
  var totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  if (currentPage > totalPages) currentPage = totalPages;
  if (currentPage < 1) currentPage = 1;
  var start = (currentPage - 1) * pageSize;
  var pageItems = items.slice(start, start + pageSize);
  if (!items.length) {
    wrap.innerHTML = '<div class="empty">No hay ventas para mostrar.</div>';
    renderPager(0, 1, 0, 0);
    return;
  }
  var html = '<table><thead><tr>';
  html += '<th style="width:32px"><input type="checkbox" class="cb-row" id="cbTodos" onchange="toggleTodos(this)"></th>';
  html += '<th>Fecha / ID</th><th>Cliente</th><th>RUT</th>';
  html += '<th>Direccion / Ciudad / Region</th><th>Total / Items</th>';
  html += '<th>Tipo</th><th>Envio</th><th>Estado / Fuente</th><th>Estado envio</th><th>Acciones</th>';
  html += '</tr></thead><tbody>';
  for (var i = 0; i < pageItems.length; i++) { html += rowHtml(pageItems[i]); }
  html += '</tbody></table>';
  wrap.innerHTML = html;
  wrap.querySelectorAll('[data-action]').forEach(function(el) {
    el.addEventListener('click', function(e) {
      e.preventDefault();
      var action = el.dataset.action;
      var id = el.dataset.id;
      if (action === 'edit') openEdit(id);
      else if (action === 'reprocesar') reprocesar(id);
      else if (action === 'recalcwc') recalcularWC(id);
      else if (action === 'autorizar') autorizar(id);
      else if (action === 'anular') anular(id);
      else if (action === 'notacredito') abrirNC(id);
      else if (action === 'adjuntarml') adjuntarMlManual(id);
      else if (action === 'adjuntarfl') adjuntarFlManual(id);
      else if (action === 'verpack') verPack(id, el.dataset.pack);
      else if (action === 'copy') { try { navigator.clipboard.writeText(id); } catch(e2) {} }
    });
  });
  renderPager(items.length, totalPages, start, pageItems.length);
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
      if (cambio) { ventas = nuevas; renderTable(); }
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

var _ncCurrentId = null;

function abrirNC(id) {
  var v = null;
  for (var i = 0; i < ventas.length; i++) {
    if (String(ventas[i].id) === String(id)) { v = ventas[i]; break; }
  }
  if (!v) return;
  _ncCurrentId = String(id);
  document.getElementById('ncModalSub').textContent = 'Venta ' + id;
  document.getElementById('ncModalInfo').innerHTML =
    '<strong>' + esc(v.cliente) + '</strong><br>RUT: ' + esc(v.rut) + '<br>Total: ' + money(v.total_bruto) + '<br>Tipo: ' + esc(v.tipo_sugerido);
  document.getElementById('ncMotivo').value = '';
  document.getElementById('ncOtro').value = '';
  document.getElementById('ncOtroGroup').style.display = 'none';
  document.getElementById('ncError').style.display = 'none';
  document.getElementById('ncParcial').checked = false;
  document.getElementById('ncLineasBox').style.display = 'none';
  document.getElementById('ncLineas').innerHTML = '';
  document.getElementById('ncModal').classList.add('open');
}

function toggleNcParcial() {
  var on = document.getElementById('ncParcial').checked;
  var box = document.getElementById('ncLineasBox');
  box.style.display = on ? 'block' : 'none';
  if (!on || !_ncCurrentId) return;
  var cont = document.getElementById('ncLineas');
  cont.innerHTML = '<div style="padding:10px;color:var(--muted)">Cargando lineas del documento...</div>';
  fetch('/ventas/' + _ncCurrentId + '/lineas-dte')
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (!data.ok || !data.lineas) { cont.innerHTML = '<div style="padding:10px;color:#f87171">' + (data.detail || 'No se pudieron leer las lineas') + '</div>'; return; }
      if (!data.lineas.length) { cont.innerHTML = '<div style="padding:10px;color:#f87171">El documento no tiene lineas</div>'; return; }
      var html = '';
      data.lineas.forEach(function(l) {
        html += '<div style="display:flex;align-items:center;gap:8px;padding:8px 10px;border-bottom:1px solid var(--border)">' +
          '<div style="flex:1;font-size:13px">' + esc(l.name) + '<div class="small">cant. facturada: ' + l.quantity + (l.sugerido ? ' &middot; <span style="color:#fbbf24">devuelto sugerido: ' + l.sugerido + '</span>' : '') + '</div></div>' +
          '<input type="number" class="nc-cred" data-idx="' + l.line_index + '" data-max="' + l.quantity + '" min="0" max="' + l.quantity + '" step="1" value="' + (l.sugerido || 0) + '" style="width:90px;padding:6px 8px">' +
          '</div>';
      });
      cont.innerHTML = html;
    })
    .catch(function(e){ cont.innerHTML = '<div style="padding:10px;color:#f87171">Error: ' + e.message + '</div>'; });
}

function cerrarNC() {
  _ncCurrentId = null;
  document.getElementById('ncModal').classList.remove('open');
}

function toggleNcOtro() {
  var val = document.getElementById('ncMotivo').value;
  document.getElementById('ncOtroGroup').style.display = val === 'otro' ? 'block' : 'none';
}

function confirmarNC() {
  if (!_ncCurrentId) return;
  var motivo = document.getElementById('ncMotivo').value;
  if (motivo === 'otro') motivo = document.getElementById('ncOtro').value.trim();
  var errDiv = document.getElementById('ncError');
  if (!motivo) { errDiv.textContent = 'Debes seleccionar o ingresar un motivo'; errDiv.style.display = 'block'; return; }

  var parcial = document.getElementById('ncParcial').checked;
  var url = '/ventas/' + _ncCurrentId + '/nota-credito';
  var payload = {motivo: motivo};
  if (parcial) {
    var lineas = [];
    var inputs = document.querySelectorAll('#ncLineas .nc-cred');
    var totalCred = 0;
    for (var i = 0; i < inputs.length; i++) {
      var cant = parseFloat(inputs[i].value || '0');
      var max = parseFloat(inputs[i].getAttribute('data-max') || '0');
      if (isNaN(cant) || cant < 0) cant = 0;
      if (cant > max) { errDiv.style.color='#f87171'; errDiv.style.display='block'; errDiv.textContent = 'No puedes acreditar mas de lo facturado en una linea'; return; }
      if (cant > 0) { lineas.push({line_index: parseInt(inputs[i].getAttribute('data-idx'), 10), cantidad: cant}); totalCred += cant; }
    }
    if (!lineas.length) { errDiv.style.color='#f87171'; errDiv.style.display='block'; errDiv.textContent = 'Indica al menos un item con cantidad > 0 para la NC parcial'; return; }
    if (!confirm('NC PARCIAL: se acreditaran ' + totalCred + ' unidad(es) en ' + lineas.length + ' linea(s). La factura original queda vigente. Continuar?')) return;
    url = '/ventas/' + _ncCurrentId + '/nota-credito-parcial';
    payload.lineas = lineas;
  }

  errDiv.textContent = (parcial ? 'Creando nota de credito PARCIAL' : 'Creando nota de credito') + ' en Odoo...';
  errDiv.style.display = 'block';
  errDiv.style.color = '#94a3b8';
  fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { cerrarNC(); alert((data.mensaje || 'Nota de credito creada en Odoo') + '. Motivo: ' + motivo); refreshData(); }
      else { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + (data.detail || 'desconocido'); }
    })
    .catch(function(e) { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + e.message; });
}

function anular(id) {
  if (!confirm('Anular el documento Odoo de la venta ' + id + ' y resetear a pendiente para reemitir?')) return;
  fetch('/ventas/' + id + '/anular', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { alert('Venta anulada. ' + (data.odoo || '') + ' Ahora puede editar y reautorizar.'); refreshData(); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
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
  document.getElementById('modalSub').textContent = 'Venta ' + v.id + (v.fuente === 'woocommerce' ? ' (WooCommerce)' : v.fuente === 'manual' ? ' (Manual)' : ' (ML)');
  document.getElementById('editId').value = v.id || '';
  document.getElementById('editTipo').value = v.tipo_sugerido || 'Boleta';
  document.getElementById('editEmail').value = v.email || '';
  document.getElementById('editCliente').value = v.cliente || '';
  document.getElementById('editRut').value = v.rut || '';
  document.getElementById('editDireccion').value = v.direccion || '';
  document.getElementById('editCiudad').value = v.ciudad || '';
  document.getElementById('editRegion').value = v.region || '';
  document.getElementById('editGiro').value = v.tipo_sugerido === 'Factura' ? (v.giro || '') : '';
  document.getElementById('editEstado').value = '';
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
    .catch(function() { document.getElementById('editProducts').value = 'Error cargando productos'; });
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

function adjuntarMlManual(id) {
  if (!confirm('Test para la venta ' + id + ':\\nSi no esta emitida, se EMITE la boleta/factura en Odoo y luego se sube el PDF a Mercado Libre. Continuar?')) return;
  fetch('/ventas/' + id + '/adjuntar-ml', {method: 'POST'})
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) {
        var extra = d.emitido_ahora ? '(se emitio ahora, move_id ' + d.move_id + ')\\n' : '';
        alert('OK. ' + extra + 'Respuesta de ML:\\n' + JSON.stringify(d.respuesta || d));
        refreshData();
      } else { alert('Error: ' + (d.detail || 'desconocido')); }
    })
    .catch(function(e){ alert('La conexion se corto antes de recibir respuesta (' + e.message + ').\\nLa carga a ML pudo haberse completado igual. Refresca y, si hace falta, vuelve a intentar (si ya estaba, ML responde "ya cargado").'); refreshData(); });
}

function adjuntarFlManual(id) {
  if (!confirm('Test para la venta ' + id + ':\\nSi no esta emitida, se EMITE la boleta/factura en Odoo y luego se sube el PDF a Falabella (SetInvoicePDF). Continuar?')) return;
  fetch('/ventas/' + id + '/adjuntar-fl', {method: 'POST'})
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) {
        var extra = d.emitido_ahora ? '(se emitio ahora, move_id ' + d.move_id + ')\\n' : '';
        alert('OK. ' + extra + 'Respuesta de Falabella:\\n' + JSON.stringify(d.respuesta || d));
        refreshData();
      } else { alert('Error: ' + (d.detail || 'desconocido')); }
    })
    .catch(function(e){ alert('Error: ' + e.message); });
}

function recalcularWC(id) {
  if (!confirm('Recalcular la venta ' + id + ' desde WooCommerce? Corrige montos (IVA + envio).')) return;
  fetch('/ventas/' + id + '/recalcular-wc', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { alert('Venta recalculada. Nuevo total: ' + money(data.total_bruto)); refreshData(); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { alert('Error: ' + e.message); });
}

var CFG_IDS = {
  mercadolibre: {b: 'cfgMLBoletas', f: 'cfgMLFacturas'},
  woocommerce:  {b: 'cfgWCBoletas', f: 'cfgWCFacturas'},
  falabella:    {b: 'cfgFLBoletas', f: 'cfgFLFacturas'}
};

var PE_IDS = {
  mercadolibre: {pagar: 'peMLpagar', adjuntar_ml: 'peMLadjuntar'},
  woocommerce:  {pagar: 'peWCpagar', email: 'peWCemail'},
  falabella:    {pagar: 'peFLpagar', email: 'peFLemail', adjuntar_fl: 'peFLadjuntar'}
};

function guardarPostEmision(fuente) {
  var ids = PE_IDS[fuente];
  if (!ids) return;
  var body = {fuente: fuente};
  if (ids.pagar) body.pagar = document.getElementById(ids.pagar).value;
  if (ids.email) body.email = document.getElementById(ids.email).value;
  if (ids.adjuntar_ml) body.adjuntar_ml = document.getElementById(ids.adjuntar_ml).value;
  if (ids.adjuntar_fl) body.adjuntar_fl = document.getElementById(ids.adjuntar_fl).value;
  var est = document.getElementById('cfgPostEstado');
  if (est) { est.textContent = 'Guardando...'; est.style.color = '#94a3b8'; }
  fetch('/config/post-emision', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }).then(function(r){ return r.json(); })
    .then(function(d) {
      if (est) {
        if (d.ok) { est.textContent = 'Guardado \\u2713 ' + fuente + ': pagar ' + d.pagar + (d.email !== undefined ? ' / email ' + d.email : '') + (d.adjuntar_ml !== undefined ? ' / adjuntar ' + d.adjuntar_ml : '') + (d.adjuntar_fl !== undefined ? ' / adjuntar ' + d.adjuntar_fl : ''); est.style.color = '#4ade80'; }
        else { est.textContent = 'Error: ' + (d.detail || 'desconocido'); est.style.color = '#f87171'; }
      }
    })
    .catch(function(e){ if (est) { est.textContent = 'Error: ' + e.message; est.style.color = '#f87171'; } });
}

var NC_IDS = {
  mercadolibre: {total: 'ncMLtotal', parcial: 'ncMLparcial'},
  woocommerce:  {total: 'ncWCtotal', parcial: 'ncWCparcial'},
  falabella:    {total: 'ncFLtotal', parcial: 'ncFLparcial'}
};

function guardarNcAuto(fuente) {
  var ids = NC_IDS[fuente];
  if (!ids) return;
  var body = {fuente: fuente,
              total: document.getElementById(ids.total).value,
              parcial: document.getElementById(ids.parcial).value};
  var est = document.getElementById('cfgNcEstado');
  if (est) { est.textContent = 'Guardando...'; est.style.color = '#94a3b8'; }
  fetch('/config/nc-auto', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }).then(function(r){ return r.json(); })
    .then(function(d) {
      if (est) {
        if (d.ok) { est.textContent = 'Guardado \\u2713 ' + fuente + ': total ' + d.total + ' / parcial ' + d.parcial; est.style.color = '#4ade80'; }
        else { est.textContent = 'Error: ' + (d.detail || 'desconocido'); est.style.color = '#f87171'; }
      }
    })
    .catch(function(e){ if (est) { est.textContent = 'Error: ' + e.message; est.style.color = '#f87171'; } });
}

function renderCafAlert(caf) {
  var el = document.getElementById('cafAlert');
  if (!el) return;
  caf = caf || {};
  var agotados = Object.keys(caf).filter(function(t){ return caf[t] === 'agotado'; });
  if (!agotados.length) { el.style.display = 'none'; el.innerHTML = ''; return; }
  var nombres = agotados.map(function(t){ return t.charAt(0).toUpperCase() + t.slice(1); }).join(' y ');
  var btns = agotados.map(function(t){ return '<button class="warn" data-caf="' + t + '">Reanudar ' + t + '</button>'; }).join(' ');
  el.style.display = 'block';
  el.innerHTML = '<div style="background:#7f1d1d;border:1px solid #ef4444;border-radius:12px;padding:12px 16px;margin-bottom:14px;color:#fecaca;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px">' +
    '<div><strong>&#9888; Sin folios CAF de ' + nombres + '.</strong> La emisi&oacute;n autom&aacute;tica de ' + nombres + ' est&aacute; DETENIDA. Solicita m&aacute;s CAF en el SII, c&aacute;rgalos en Odoo y pulsa Reanudar. Las ventas afectadas quedaron <strong>pendientes</strong>.</div>' +
    '<div style="display:flex;gap:6px;flex-wrap:wrap">' + btns + '</div></div>';
  el.querySelectorAll('[data-caf]').forEach(function(b){ b.addEventListener('click', function(){ reanudarCaf(b.getAttribute('data-caf')); }); });
}

function reanudarCaf(tipo) {
  if (!confirm('Ya cargaste mas folios CAF de ' + tipo + '? Se reanuda la emision automatica de ' + tipo + '.')) return;
  fetch('/config/caf/reanudar', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({tipo: tipo})})
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (d.ok) { renderCafAlert(d.caf); alert('Reanudado. Ahora autoriza o reprocesa las ventas que quedaron pendientes por falta de CAF.'); refreshData(); }
      else { alert('Error: ' + (d.detail || 'desconocido')); }
    })
    .catch(function(e){ alert('Error: ' + e.message); });
}

function pollCaf() {
  fetch('/config/auto-emision').then(function(r){ return r.json(); }).then(function(d){ renderCafAlert(d.caf); }).catch(function(){});
}

function cargarAutoEmision() {
  fetch('/config/auto-emision')
    .then(function(r){ return r.json(); })
    .then(function(d) {
      renderCafAlert(d.caf);
      Object.keys(CFG_IDS).forEach(function(fuente) {
        var cfg = d[fuente];
        if (!cfg) return;
        var ids = CFG_IDS[fuente];
        if (cfg.boletas) document.getElementById(ids.b).value = cfg.boletas;
        if (cfg.facturas) document.getElementById(ids.f).value = cfg.facturas;
      });
      var pe = d.post_emit || {};
      Object.keys(PE_IDS).forEach(function(fuente) {
        var c = pe[fuente];
        if (!c) return;
        var ids = PE_IDS[fuente];
        if (ids.pagar && c.pagar) document.getElementById(ids.pagar).value = c.pagar;
        if (ids.email && c.email) { var _e = document.getElementById(ids.email); if (_e) _e.value = c.email; }
        if (ids.adjuntar_ml && c.adjuntar_ml) { var _a = document.getElementById(ids.adjuntar_ml); if (_a) _a.value = c.adjuntar_ml; }
        if (ids.adjuntar_fl && c.adjuntar_fl) { var _f = document.getElementById(ids.adjuntar_fl); if (_f) _f.value = c.adjuntar_fl; }
      });
      var nc = d.nc_auto || {};
      Object.keys(NC_IDS).forEach(function(fuente) {
        var c = nc[fuente];
        if (!c) return;
        var ids = NC_IDS[fuente];
        if (c.total) { var _t = document.getElementById(ids.total); if (_t) _t.value = c.total; }
        if (c.parcial) { var _p = document.getElementById(ids.parcial); if (_p) _p.value = c.parcial; }
      });
      var est = document.getElementById('cfgEstado');
      if (est) est.textContent = '';
    })
    .catch(function(){});
}

function guardarAutoEmision(fuente) {
  var ids = CFG_IDS[fuente];
  if (!ids) return;
  var b = document.getElementById(ids.b).value;
  var f = document.getElementById(ids.f).value;
  var est = document.getElementById('cfgEstado');
  if (est) { est.textContent = 'Guardando...'; est.style.color = '#94a3b8'; }
  fetch('/config/auto-emision', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({fuente: fuente, boletas: b, facturas: f})
  }).then(function(r){ return r.json(); })
    .then(function(d) {
      if (est) {
        if (d.ok) { est.textContent = 'Guardado \\u2713 ' + fuente + ': boletas ' + d.boletas + ' / facturas ' + d.facturas; est.style.color = '#4ade80'; }
        else { est.textContent = 'Error: ' + (d.detail || 'desconocido'); est.style.color = '#f87171'; }
      }
    })
    .catch(function(e){ if (est) { est.textContent = 'Error: ' + e.message; est.style.color = '#f87171'; } });
}

function recalcularWCTodos() {
  var wcPend = ventas.filter(function(v){ return v.fuente === 'woocommerce' && (v.estado === 'pendiente' || v.estado === 'error'); });
  if (!wcPend.length) { alert('No hay ventas WooCommerce pendientes/con error para recalcular'); return; }
  if (!confirm('Recalcular ' + wcPend.length + ' venta(s) WooCommerce pendientes/con error desde la web? (corrige montos IVA + envio)')) return;
  var btn = document.querySelector('[onclick="recalcularWCTodos()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Recalculando...'; }
  fetch('/wc/recalcular-pendientes', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.innerHTML = '&#128260; Recalcular WC'; }
      if (data.ok) { alert(data.mensaje); if (data.total > 0) { setTimeout(refreshData, 10000); setTimeout(refreshData, 30000); } }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.innerHTML = '&#128260; Recalcular WC'; } alert('Error: ' + e.message); });
}

function cambiarEstadoActual() {
  if (!currentId) return;
  var nuevo = document.getElementById('editEstado').value;
  if (!nuevo) { alert('Selecciona un estado'); return; }
  if (!confirm('Cambiar el estado de la venta ' + currentId + ' a "' + nuevo + '"? (Solo actualiza el registro local, no modifica Odoo)')) return;
  fetch('/ventas/' + currentId + '/estado', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({estado: nuevo})
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { alert('Estado actualizado a ' + data.estado); closeModal(); refreshData(); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { alert('Error: ' + e.message); });
}

function onCheckboxChange() {
  var seleccionadas = getSeleccionadas();
  var btn = document.getElementById('btnAgrupar');
  var btnMasivo = document.getElementById('btnAutorizarMasivo');
  if (btnMasivo) btnMasivo.style.display = seleccionadas.length >= 1 ? 'inline-block' : 'none';
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
    count.textContent = seleccionadas.length === 1 ? '1 venta seleccionada' : '';
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
  if (!confirm('Agrupar ' + ids.length + ' ventas en una sola boleta/factura?')) return;
  fetch('/ventas/agrupar', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ids: ids})
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        alert('Ventas agrupadas en ' + data.venta_principal + '. Items: ' + data.total_items);
        document.getElementById('btnAgrupar').style.display = 'none';
        document.getElementById('selInfo').style.display = 'none';
        refreshData();
      } else {
        alert('Error: ' + (data.detail || 'desconocido'));
      }
    });
}

function autorizarMasivo() {
  var ids = getSeleccionadas();
  if (!ids.length) { alert('Selecciona al menos 1 venta'); return; }
  if (!confirm('Autorizar y emitir en Odoo ' + ids.length + ' venta(s) seleccionada(s)?')) return;
  var btn = document.getElementById('btnAutorizarMasivo');
  if (btn) { btn.disabled = true; btn.textContent = 'Emitiendo...'; }
  fetch('/ventas/autorizar-masivo', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ids: ids})
  }).then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.innerHTML = '&#10003; Autorizar seleccionadas'; }
      if (typeof data.ok === 'number') { alert('Emitidas: ' + data.ok + ' | Errores: ' + data.error); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
      refreshData();
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.innerHTML = '&#10003; Autorizar seleccionadas'; } alert('Error: ' + e.message); });
}

function actualizarEnvio() {
  var btn = document.querySelector('[onclick="actualizarEnvio()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Consultando...'; }
  fetch('/ventas/actualizar-envio', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Actualizar envios'; }
      if (data.ok) { alert(data.mensaje); if (data.procesando > 0) { setTimeout(refreshData, 30000); setTimeout(refreshData, 90000); } }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Actualizar envios'; } alert('Error: ' + e.message); });
}

function reconciliarML() {
  var btn = document.querySelector('[onclick="reconciliarML()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Consultando ML...'; }
  fetch('/ventas/reconciliar', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar ML'; }
      if (data.ok) {
        var msg = 'ML: ' + data.ordenes_ml + ' | BD: ' + data.en_bd + ' | Faltantes: ' + data.faltantes;
        if (data.faltantes > 0) { msg += ' - ' + data.mensaje; setTimeout(refreshData, 10000); setTimeout(refreshData, 30000); }
        alert(msg);
      } else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar ML'; } alert('Error: ' + e.message); });
}

function revisarCanceladasML() {
  if (!confirm('Revisar las ordenes ML canceladas recientes y crear NC de las que ya estaban facturadas (incluye ventas divididas)?\\n(Idempotente y respeta el interruptor de NC total de ML.)')) return;
  var btn = document.querySelector('[onclick="revisarCanceladasML()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Revisando...'; }
  fetch('/ml/revisar-canceladas', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Canceladas/NC ML'; }
      if (data.ok) { alert(data.mensaje || 'Revisando canceladas ML.'); setTimeout(refreshData, 10000); setTimeout(refreshData, 30000); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Canceladas/NC ML'; } alert('Error: ' + e.message); });
}

function reconciliarWC() {
  var btn = document.querySelector('[onclick="reconciliarWC()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Consultando WC...'; }
  fetch('/wc/reconciliar', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar WC'; }
      if (data.ok) {
        var msg = 'WC: ' + data.total_wc + ' | BD: ' + data.en_bd + ' | Faltantes: ' + data.faltantes;
        if (data.faltantes > 0) { msg += ' - ' + data.mensaje; setTimeout(refreshData, 8000); setTimeout(refreshData, 20000); }
        alert(msg);
      } else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar WC'; } alert('Error: ' + e.message); });
}

function reconciliarFL() {
  var dias = prompt('Reconciliar Falabella: cuantos dias hacia atras buscar ordenes?\\n(vacio = ventana por defecto)', '30');
  if (dias === null) return;
  var url = '/fl/reconciliar?limit=100';
  dias = (dias || '').trim();
  if (dias) url += '&days=' + encodeURIComponent(dias);
  var btn = document.querySelector('[onclick="reconciliarFL()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Consultando FL...'; }
  fetch(url, {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar FL'; }
      if (data.ok) {
        var msg = 'FL: ' + data.total_fl + ' | BD: ' + data.en_bd + ' | Faltantes: ' + data.faltantes;
        if (data.faltantes > 0) { msg += ' - ' + data.mensaje; setTimeout(refreshData, 8000); setTimeout(refreshData, 20000); }
        else { msg += '\\n(No hay ordenes nuevas en esa ventana. Prueba mas dias, o usa "Ingresar orden FL" con un OrderId.)'; }
        alert(msg);
      } else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Reconciliar FL'; } alert('Error: ' + e.message); });
}

function revisarDevolucionesFL() {
  if (!confirm('Revisar las ventas de Falabella EMITIDAS y crear Nota de Credito en las que la orden fue cancelada o devuelta totalmente?\\n(Idempotente: no repite NC. Las devoluciones parciales quedan en el log para hacerlas a mano.)')) return;
  var btn = document.querySelector('[onclick="revisarDevolucionesFL()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Revisando...'; }
  fetch('/fl/revisar-devoluciones', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Devoluciones/Cancelaciones FL'; }
      if (data.ok) { alert(data.mensaje || ('Revisando ' + data.total_emitidas + ' ventas FL emitidas.')); setTimeout(refreshData, 10000); setTimeout(refreshData, 25000); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Devoluciones/Cancelaciones FL'; } alert('Error: ' + e.message); });
}

function reprocesarDatosFL() {
  if (!confirm('Reprocesar TODAS las ventas de Falabella?\\nReconsulta cada orden y actualiza envio + telefono. Los DTE ya emitidos no cambian de monto (no se puede), pero las pendientes quedaran con envio + telefono correctos y se actualiza el telefono del cliente en Odoo.')) return;
  var btn = document.querySelector('[onclick="reprocesarDatosFL()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Reprocesando...'; }
  fetch('/fl/reprocesar-datos', {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Reprocesar datos FL'; }
      if (data.ok) { alert(data.mensaje || ('Reprocesando ' + data.total + ' ventas FL.')); setTimeout(refreshData, 8000); setTimeout(refreshData, 20000); }
      else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Reprocesar datos FL'; } alert('Error: ' + e.message); });
}

function ingresarFL() {
  var oid = prompt('Ingresar una orden de Falabella por su ID (OrderId numerico de Seller Center):');
  if (oid === null) return;
  oid = (oid || '').trim().replace(/^FL-/, '');
  if (!oid) { alert('Debes indicar el OrderId'); return; }
  var btn = document.querySelector('[onclick="ingresarFL()"]');
  if (btn) { btn.disabled = true; btn.textContent = 'Ingresando...'; }
  fetch('/fl/ingresar/' + encodeURIComponent(oid), {method:'POST'})
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (btn) { btn.disabled = false; btn.textContent = 'Ingresar orden FL'; }
      if (data.ok) {
        alert('Orden ' + (data.id || ('FL-' + oid)) + ' procesada.\\nCliente: ' + (data.cliente || '-') + ' | Estado: ' + (data.estado || data.message || '-'));
        setFuente('falabella');
        refreshData();
      } else { alert('Error: ' + (data.detail || 'desconocido')); }
    })
    .catch(function(e) { if (btn) { btn.disabled = false; btn.textContent = 'Ingresar orden FL'; } alert('Error: ' + e.message); });
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
        html += '<div style="display:flex;justify-content:space-between;margin-bottom:8px;"><strong>Order ID: ' + safe(o.id) + '</strong>';
        html += '<span style="color:#94a3b8">' + safe(o.status) + ' - ' + o.item_count + ' item(s) - <strong>' + money(o.total) + '</strong></span></div>';
        html += '<ul style="margin:0;padding-left:18px;">';
        (o.items || []).forEach(function(item) { html += '<li style="font-size:13px;color:#94a3b8;margin-bottom:3px;">' + safe(item) + '</li>'; });
        html += '</ul></div>';
      }
      if (!ordenes.length) html = '<p style="color:#94a3b8">No se encontraron ordenes.</p>';
      document.getElementById('packModalBody').innerHTML = html;
    })
    .catch(function(e) { document.getElementById('packModalBody').innerHTML = '<p style="color:#f87171">Error: ' + e.message + '</p>'; });
}

function closePackModal() {
  document.getElementById('packModal').classList.remove('open');
}

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
  var lbl = document.getElementById('calYearLabel');
  var conteo = {};
  ventas.forEach(function(v) {
    var k = getTurnoKey(v.creado_en);
    if (k) conteo[k] = (conteo[k] || 0) + 1;
  });
  var keys = Object.keys(conteo);
  if (!keys.length) {
    grid.innerHTML = '<div style="grid-column:1/-1;color:#94a3b8;padding:20px">No hay ventas para mostrar</div>';
    if (lbl) lbl.textContent = '';
    return;
  }
  if (calYear == null) {
    calYear = Math.max.apply(null, keys.map(function(k) { return parseInt(k.split('-')[0], 10); }));
  }
  var meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
  var diasSem = ['L','M','X','J','V','S','D'];
  var html = '';
  var totalAnio = 0;
  for (var mes = 0; mes < 12; mes++) {
    var totalMes = 0;
    for (var kk in conteo) {
      var pk = kk.split('-');
      if (parseInt(pk[0], 10) === calYear && (parseInt(pk[1], 10) - 1) === mes) totalMes += conteo[kk];
    }
    totalAnio += totalMes;
    html += '<div style="background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:8px">';
    html += '<div style="font-size:12px;font-weight:700;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center"><span>' + meses[mes] + '</span><span style="font-size:11px;color:' + (totalMes ? '#4ade80' : '#475569') + '">' + totalMes + '</span></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(7,1fr);gap:2px;text-align:center">';
    for (var w = 0; w < 7; w++) html += '<div style="font-size:9px;color:#64748b">' + diasSem[w] + '</div>';
    var first = new Date(Date.UTC(calYear, mes, 1));
    var dow = (first.getUTCDay() + 6) % 7;
    for (var b = 0; b < dow; b++) html += '<div></div>';
    var daysInMonth = new Date(Date.UTC(calYear, mes + 1, 0)).getUTCDate();
    for (var dia = 1; dia <= daysInMonth; dia++) {
      var key = calYear + '-' + String(mes + 1).padStart(2, '0') + '-' + String(dia).padStart(2, '0');
      var cnt = conteo[key] || 0;
      var isActive = turnoActivo === key;
      var bg = isActive ? 'var(--blue)' : (cnt > 0 ? '#14532d' : 'transparent');
      var col = isActive ? 'white' : (cnt > 0 ? '#86efac' : '#475569');
      var cursor = cnt > 0 ? 'pointer' : 'default';
      var attr = cnt > 0 ? ' data-turno="' + key + '" title="' + cnt + ' venta(s)"' : '';
      html += '<div class="cal-day" style="font-size:10px;padding:3px 0;border-radius:4px;background:' + bg + ';color:' + col + ';cursor:' + cursor + '"' + attr + '>' + dia + '</div>';
    }
    html += '</div></div>';
  }
  if (lbl) lbl.textContent = calYear + ' (' + totalAnio + ' ventas)';
  grid.innerHTML = html;
  grid.querySelectorAll('[data-turno]').forEach(function(el) {
    el.addEventListener('click', function() { seleccionarTurno(el.dataset.turno); });
  });
}

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
    tipo: tipo, order_id: document.getElementById('vmOrderId').value.trim() || null,
    cliente: nombre, rut: rut, email: email,
    direccion: document.getElementById('vmDireccion').value.trim(),
    ciudad: document.getElementById('vmCiudad').value.trim(),
    region: document.getElementById('vmRegion').value,
    giro: tipo === 'Factura' ? document.getElementById('vmGiro').value.trim() : '',
    productos: productos, autorizar: autorizar
  };
}

function guardarVentaManual() {
  var payload = buildVentaManualPayload(false);
  var errDiv = document.getElementById('vmError');
  if (!payload) { errDiv.textContent = 'Nombre, RUT y email son obligatorios'; errDiv.style.display = 'block'; return; }
  errDiv.textContent = 'Guardando...'; errDiv.style.display = 'block'; errDiv.style.color = '#94a3b8';
  fetch('/ventas/manual', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) })
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { cerrarIngresarVenta(); alert('Venta ingresada: ' + data.id); refreshData(); }
      else { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + (data.detail || 'desconocido'); }
    })
    .catch(function(e) { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + e.message; });
}

function guardarYAutorizarManual() {
  var payload = buildVentaManualPayload(true);
  var errDiv = document.getElementById('vmError');
  if (!payload) { errDiv.textContent = 'Nombre, RUT y email son obligatorios'; errDiv.style.display = 'block'; return; }
  errDiv.textContent = 'Guardando y autorizando en Odoo...'; errDiv.style.display = 'block'; errDiv.style.color = '#94a3b8';
  fetch('/ventas/manual', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) })
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) {
        cerrarIngresarVenta();
        if (data.autorizado) { alert('Venta autorizada en Odoo: move_id=' + data.move_id); }
        else { alert('Venta ingresada: ' + data.id); }
        refreshData();
      } else { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + (data.detail || 'desconocido'); }
    })
    .catch(function(e) { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + e.message; });
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
  if (!nombre || !rut || !email) { errDiv.textContent = 'Nombre, RUT y email son obligatorios'; errDiv.style.display = 'block'; errDiv.style.color = '#f87171'; return; }
  var esEmpresa = document.getElementById('cliTipo').value === 'empresa';
  var payload = {
    nombre: nombre, rut: rut, email: email,
    direccion: document.getElementById('cliDireccion').value.trim(),
    ciudad: document.getElementById('cliCiudad').value.trim(),
    region: document.getElementById('cliRegion').value,
    giro: esEmpresa ? document.getElementById('cliGiro').value.trim() : '',
    es_empresa: esEmpresa
  };
  errDiv.textContent = 'Creando...'; errDiv.style.display = 'block'; errDiv.style.color = '#94a3b8';
  fetch('/clientes/crear', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) })
    .then(function(r){ return r.json(); })
    .then(function(data) {
      if (data.ok) { cerrarCrearCliente(); alert('Cliente creado en Odoo: ' + data.nombre + ' (id=' + data.partner_id + ')'); }
      else { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + (data.detail || 'desconocido'); }
    })
    .catch(function(e) { errDiv.style.color = '#f87171'; errDiv.textContent = 'Error: ' + e.message; });
}

document.getElementById('editModal').addEventListener('click', function(e) { if (e.target.id === 'editModal') closeModal(); });
document.getElementById('packModal').addEventListener('click', function(e) { if (e.target.id === 'packModal') closePackModal(); });
document.getElementById('calModal').addEventListener('click', function(e) { if (e.target.id === 'calModal') cerrarCalendario(); });
document.getElementById('clienteModal').addEventListener('click', function(e) { if (e.target.id === 'clienteModal') cerrarCrearCliente(); });
document.getElementById('ncModal').addEventListener('click', function(e) { if (e.target.id === 'ncModal') cerrarNC(); });
document.getElementById('ventaManualModal').addEventListener('click', function(e) { if (e.target.id === 'ventaManualModal') cerrarIngresarVenta(); });

refreshData();
cargarAutoEmision();
setInterval(function() {
  var anyOpen = ['editModal','packModal','calModal','clienteModal'].some(function(id) {
    return document.getElementById(id).classList.contains('open');
  });
  if (!anyOpen) refreshSilente();
  pollCaf();
}, 30000);
'''

@app.get("/mi-ip")
def mi_ip():
    try:
        res = requests.get("https://ifconfig.me", headers={"User-Agent": "curl/7"}, timeout=10)
        return {"ip": res.text.strip()}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ui/app.js")
def ui_js():
    from fastapi.responses import Response
    return Response(content=UI_JS, media_type="application/javascript; charset=utf-8")

@app.get("/ui", response_class=HTMLResponse)
def ui_bandeja():
    return HTMLResponse(content=UI_HTML)
