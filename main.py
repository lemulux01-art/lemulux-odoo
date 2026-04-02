from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import os
import time
import threading
import logging
import requests
import xmlrpc.client
from typing import Optional, Any
from dataclasses import dataclass
from threading import Lock

# =========================================================
# CONFIG
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lemulux")

app = FastAPI()

IVA_RATE = 1.19
ML_DEFAULT_EMAIL = "boleta@lemulux.com"
DEFAULT_BOLETA_ACTIVITY = "(boleta)"

# Odoo Chile
SII_TAXPAYER_EMPRESA = "1"       # 1a categoría
SII_TAXPAYER_CONSUMIDOR = "4"    # consumidor final

REQUIRED_ENV_VARS = [
    "ML_ACCESS_TOKEN",
    "ML_REFRESH_TOKEN",
    "ML_CLIENT_ID",
    "ML_CLIENT_SECRET",
    "ML_REDIRECT_URI",
    "ODOO_URL",
    "ODOO_DB",
    "ODOO_USER",
    "ODOO_API_KEY",
    "ODOO_DOC_TYPE_FACTURA_ID",
    "ODOO_DOC_TYPE_BOLETA_ID",
]

# Evita tormenta de webhooks duplicados
RECENT_ORDERS: dict[str, float] = {}
RECENT_ORDERS_LOCK = Lock()
DEDUP_SECONDS = 60

# Renovación automática del token cada 5 horas
TOKEN_REFRESH_INTERVAL = 5 * 60 * 60


# =========================================================
# HELPERS
# =========================================================

def get_env(name: str, required: bool = True) -> str:
    value = os.getenv(name)
    if value is None:
        if required:
            raise RuntimeError(f"Falta variable de entorno: {name}")
        return ""
    value = value.strip()
    if required and not value:
        raise RuntimeError(f"Variable vacía: {name}")
    return value


def to_int_env(name: str, required: bool = True) -> Optional[int]:
    raw = get_env(name, required=required)
    if not raw:
        return None
    return int(raw)


def normalize_rut(rut: str) -> str:
    if not rut:
        return ""
    return rut.strip().upper().replace(".", "").replace(" ", "")


def first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value:
            return value
    return ""


def recursive_find_first(data: Any, keys: list[str]) -> str:
    if isinstance(data, dict):
        for key, value in data.items():
            if key in keys and isinstance(value, str) and value.strip():
                return value.strip()
            found = recursive_find_first(value, keys)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = recursive_find_first(item, keys)
            if found:
                return found
    return ""


def should_skip_recent_order(order_id: str) -> bool:
    now = time.time()
    with RECENT_ORDERS_LOCK:
        last = RECENT_ORDERS.get(order_id)
        if last and now - last < DEDUP_SECONDS:
            return True
        RECENT_ORDERS[order_id] = now
        return False


# =========================================================
# STARTUP
# =========================================================

def schedule_token_refresh():
    while True:
        time.sleep(TOKEN_REFRESH_INTERVAL)
        logger.info("⏰ Renovación programada del token ML (cada 5h)...")
        success = refresh_ml_token()
        if not success:
            logger.error("❌ Falló la renovación programada del token ML")


@app.on_event("startup")
async def on_startup():
    missing = [k for k in REQUIRED_ENV_VARS if not os.getenv(k, "").strip()]
    if missing:
        raise RuntimeError(f"Variables faltantes: {missing}")
    logger.info("✅ Variables de entorno cargadas correctamente")

    t = threading.Thread(target=schedule_token_refresh, daemon=True)
    t.start()
    logger.info("⏰ Renovación automática del token ML iniciada (cada 5 horas)")


# =========================================================
# ENDPOINTS BASE
# =========================================================

@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/ml/diagnostico")
def diagnostico():
    return {
        "service": "ok",
        "env_ok": True,
        "ml_token_loaded": bool(os.getenv("ML_ACCESS_TOKEN")),
        "refresh_token_loaded": bool(os.getenv("ML_REFRESH_TOKEN")),
    }


# =========================================================
# AUTH MERCADO LIBRE
# =========================================================

def persist_tokens_in_memory(access_token: str, refresh_token: str):
    os.environ["ML_ACCESS_TOKEN"] = access_token
    os.environ["ML_REFRESH_TOKEN"] = refresh_token


def refresh_ml_token() -> bool:
    try:
        payload = {
            "grant_type": "refresh_token",
            "client_id": get_env("ML_CLIENT_ID"),
            "client_secret": get_env("ML_CLIENT_SECRET"),
            "refresh_token": get_env("ML_REFRESH_TOKEN"),
        }
        response = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        new_access_token = data.get("access_token")
        new_refresh_token = data.get("refresh_token") or get_env("ML_REFRESH_TOKEN")

        if not new_access_token:
            logger.error("No vino access_token al refrescar")
            return False

        persist_tokens_in_memory(new_access_token, new_refresh_token)
        logger.info("✅ Token ML renovado en memoria")
        return True

    except Exception as e:
        logger.error(f"❌ Error renovando token ML: {e}", exc_info=True)
        return False


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")

    try:
        payload = {
            "grant_type": "authorization_code",
            "client_id": get_env("ML_CLIENT_ID"),
            "client_secret": get_env("ML_CLIENT_SECRET"),
            "code": code,
            "redirect_uri": get_env("ML_REDIRECT_URI"),
        }
        response = requests.post(
            "https://api.mercadolibre.com/oauth/token",
            data=payload,
            timeout=30,
        )
        try:
            data = response.json()
        except Exception:
            data = {"raw": response.text}

        if response.status_code == 200:
            access_token = data.get("access_token", "")
            refresh_token = data.get("refresh_token", "")
            if access_token:
                os.environ["ML_ACCESS_TOKEN"] = access_token
            if refresh_token:
                os.environ["ML_REFRESH_TOKEN"] = refresh_token
            logger.info("✅ Tokens OAuth guardados en memoria")

        return JSONResponse(
            status_code=response.status_code,
            content={"status_code": response.status_code, "response": data},
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "No se pudo conectar con Mercado Libre", "detail": str(e)},
        )


@app.post("/ml/refresh-token")
def manual_refresh_token():
    success = refresh_ml_token()
    if success:
        return {"ok": True, "message": "Token renovado"}
    return JSONResponse(
        status_code=500,
        content={"ok": False, "message": "No se pudo renovar el token"},
    )


# =========================================================
# MERCADO LIBRE API
# =========================================================

def ml_headers() -> dict:
    return {"Authorization": f"Bearer {get_env('ML_ACCESS_TOKEN')}"}


def ml_get(url: str, retries: int = 4) -> dict:
    for attempt in range(retries):
        response = requests.get(url, headers=ml_headers(), timeout=30)

        if response.status_code == 401:
            logger.warning(f"401 en {url}. Intentando refresh token...")
            if refresh_ml_token():
                response = requests.get(url, headers=ml_headers(), timeout=30)
            else:
                raise Exception("Token ML inválido o expirado")

        if response.status_code == 429:
            wait_time = 2 * (attempt + 1)
            logger.warning(f"429 en {url}. Reintento en {wait_time}s")
            time.sleep(wait_time)
            continue

        response.raise_for_status()
        return response.json()

    raise Exception(f"ML devolvió 429 demasiadas veces para {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_shipment(shipment_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/shipments/{shipment_id}")


def get_ml_billing_info(order_id: str) -> dict:
    url = f"https://api.mercadolibre.com/orders/{order_id}/billing_info"
    for attempt in range(4):
        response = requests.get(url, headers=ml_headers(), timeout=30)

        if response.status_code == 401:
            logger.warning(f"401 en billing_info {order_id}. Intentando refresh token...")
            if refresh_ml_token():
                response = requests.get(url, headers=ml_headers(), timeout=30)
            else:
                raise Exception("Token ML inválido o expirado")

        if response.status_code == 404:
            return {}

        if response.status_code == 429:
            wait_time = 2 * (attempt + 1)
            logger.warning(f"429 en billing_info {order_id}. Reintento en {wait_time}s")
            time.sleep(wait_time)
            continue

        response.raise_for_status()
        return response.json()

    raise Exception(f"ML devolvió 429 demasiadas veces en billing_info para {order_id}")


# =========================================================
# EXTRACCIÓN BILLING_INFO
# =========================================================

def extract_rut_from_billing_info(billing: dict) -> str:
    candidates = [
        recursive_find_first(billing, ["doc_number"]),
        recursive_find_first(billing, ["document_number"]),
        recursive_find_first(billing, ["vat"]),
        recursive_find_first(billing, ["rut"]),
    ]
    for candidate in candidates:
        rut = normalize_rut(candidate or "")
        if rut:
            return rut
    return ""


def extract_name_from_billing_info(billing: dict, buyer: dict) -> str:
    return first_non_empty(
        recursive_find_first(billing, ["name"]),
        recursive_find_first(billing, ["social_reason"]),
        recursive_find_first(billing, ["razon_social"]),
        recursive_find_first(billing, ["business_name"]),
        buyer.get("first_name"),
        buyer.get("nickname"),
        "Cliente Mercado Libre",
    )


def extract_activity_from_billing_info(billing: dict) -> str:
    activity = first_non_empty(
        recursive_find_first(billing, ["business_activity"]),
        recursive_find_first(billing, ["activity"]),
        recursive_find_first(billing, ["economic_activity"]),
        recursive_find_first(billing, ["taxpayer_activity"]),
        recursive_find_first(billing, ["activity_description"]),
        recursive_find_first(billing, ["giro"]),
        recursive_find_first(billing, ["description_of_activity"]),
    )
    return activity.strip() if activity else ""


def extract_taxpayer_type_from_billing_info(billing: dict) -> str:
    return first_non_empty(
        recursive_find_first(billing, ["taxpayer_type"]),
        recursive_find_first(billing, ["taxpayer_kind"]),
        recursive_find_first(billing, ["taxpayer_category"]),
        recursive_find_first(billing, ["contributor_type"]),
        recursive_find_first(billing, ["tipo_contribuyente"]),
        recursive_find_first(billing, ["description"]),
    ).strip().lower()


def ml_indicates_final_consumer(billing: dict) -> bool:
    t = extract_taxpayer_type_from_billing_info(billing)
    return any(x in t for x in ["consumidor final", "final consumer", "consumer final", "cf"])


def should_be_company_from_ml(billing: dict, buyer: dict) -> bool:
    if ml_indicates_final_consumer(billing):
        return False
    if extract_activity_from_billing_info(billing):
        return True
    name = extract_name_from_billing_info(billing, buyer).upper()
    return any(m in name for m in ["SPA", "EIRL", "LTDA", "S.A", "SOCIEDAD", "COMERCIAL"])


# =========================================================
# ODOO
# =========================================================

@dataclass
class OdooCtx:
    db: str
    api_key: str
    uid: int
    models: Any


def odoo_connect() -> OdooCtx:
    common = xmlrpc.client.ServerProxy(f"{get_env('ODOO_URL')}/xmlrpc/2/common")
    uid = common.authenticate(
        get_env("ODOO_DB"),
        get_env("ODOO_USER"),
        get_env("ODOO_API_KEY"),
        {},
    )
    if not uid:
        raise Exception("No se pudo autenticar en Odoo")
    models = xmlrpc.client.ServerProxy(f"{get_env('ODOO_URL')}/xmlrpc/2/object")
    return OdooCtx(
        db=get_env("ODOO_DB"),
        api_key=get_env("ODOO_API_KEY"),
        uid=uid,
        models=models,
    )


def odoo_exec(ctx: OdooCtx, model: str, method: str, args: list, kwargs: dict = None) -> Any:
    return ctx.models.execute_kw(
        ctx.db, ctx.uid, ctx.api_key,
        model, method, args, kwargs or {},
    )


def model_has_field(ctx: OdooCtx, model: str, field_name: str) -> bool:
    fields_info = odoo_exec(ctx, model, "fields_get", [], {"attributes": ["type"]})
    return field_name in fields_info


def get_chile_country_id(ctx: OdooCtx) -> int:
    ids = odoo_exec(ctx, "res.country", "search", [[["code", "=", "CL"]]], {"limit": 1})
    if not ids:
        raise Exception("No se encontró Chile en Odoo")
    return ids[0]


def get_rut_identification_type_id(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(
        ctx, "l10n_latam.identification.type", "search",
        [[["name", "ilike", "RUT"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def read_partner(ctx: OdooCtx, partner_id: int) -> dict:
    fields = [
        "name", "vat", "email", "l10n_cl_dte_email", "comment",
        "company_type", "is_company", "country_id", "l10n_cl_sii_taxpayer_type",
    ]
    if model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        fields.append("l10n_latam_identification_type_id")
    data = odoo_exec(ctx, "res.partner", "read", [[partner_id], fields])
    return data[0] if data else {}


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut = normalize_rut(rut)
    if not rut:
        return None
    ids = odoo_exec(ctx, "res.partner", "search", [[["vat", "=", rut]]], {"limit": 1})
    return ids[0] if ids else None


def find_partner_by_buyer_id(ctx: OdooCtx, buyer_id: str) -> Optional[int]:
    if not buyer_id:
        return None
    ids = odoo_exec(
        ctx, "res.partner", "search",
        [[["comment", "ilike", f"ML_BUYER_ID:{buyer_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def append_ml_buyer_id_to_partner(ctx: OdooCtx, partner_id: int, buyer_id: str):
    if not buyer_id:
        return
    partner_data = read_partner(ctx, partner_id)
    current_comment = partner_data.get("comment") or ""
    marker = f"ML_BUYER_ID:{buyer_id}"
    if marker in current_comment:
        return
    new_comment = f"{current_comment}\n{marker}".strip()
    odoo_exec(ctx, "res.partner", "write", [[partner_id], {"comment": new_comment}])


def build_partner_vals_from_ml(ctx: OdooCtx, buyer: dict, billing: dict, rut: str) -> dict:
    chile_country_id = get_chile_country_id(ctx)
    rut_identification_type_id = get_rut_identification_type_id(ctx)
    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    vals = {
        "name": extract_name_from_billing_info(billing, buyer),
        "vat": rut or False,
        "email": ML_DEFAULT_EMAIL,
        "l10n_cl_dte_email": ML_DEFAULT_EMAIL,
        "customer_rank": 1,
        "company_type": "company" if is_company else "person",
        "is_company": is_company,
        "country_id": chile_country_id,
        "l10n_cl_sii_taxpayer_type": sii_taxpayer_type,
    }

    if rut and rut_identification_type_id and model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        vals["l10n_latam_identification_type_id"] = rut_identification_type_id

    activity = extract_activity_from_billing_info(billing)
    if not activity and not is_company:
        if model_has_field(ctx, "res.partner", "activity_description"):
            vals["activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_studio_activity_description"):
            vals["x_studio_activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_giro"):
            vals["x_giro"] = DEFAULT_BOLETA_ACTIVITY

    return vals


def update_partner_if_needed(ctx: OdooCtx, partner_id: int, buyer: dict, billing: dict, rut: str):
    current = read_partner(ctx, partner_id)
    vals = {}

    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    if not current.get("vat") and rut:
        vals["vat"] = rut

    if not current.get("l10n_cl_sii_taxpayer_type"):
        vals["l10n_cl_sii_taxpayer_type"] = sii_taxpayer_type

    if not current.get("email"):
        vals["email"] = ML_DEFAULT_EMAIL

    if not current.get("l10n_cl_dte_email"):
        vals["l10n_cl_dte_email"] = ML_DEFAULT_EMAIL

    if not current.get("country_id"):
        vals["country_id"] = get_chile_country_id(ctx)

    activity = extract_activity_from_billing_info(billing)
    if not activity and not is_company:
        if model_has_field(ctx, "res.partner", "activity_description"):
            vals["activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_studio_activity_description"):
            vals["x_studio_activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_giro"):
            vals["x_giro"] = DEFAULT_BOLETA_ACTIVITY

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner {partner_id} actualizado con campos DTE: {list(vals.keys())}")


def ensure_partner_dte_fields(ctx: OdooCtx, partner_id: int, billing: dict, buyer: dict):
    current = read_partner(ctx, partner_id)
    vals = {}

    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    if not current.get("email"):
        vals["email"] = ML_DEFAULT_EMAIL

    if not current.get("l10n_cl_dte_email"):
        vals["l10n_cl_dte_email"] = ML_DEFAULT_EMAIL

    if not current.get("country_id"):
        vals["country_id"] = get_chile_country_id(ctx)

    if not current.get("l10n_cl_sii_taxpayer_type"):
        vals["l10n_cl_sii_taxpayer_type"] = sii_taxpayer_type

    activity = extract_activity_from_billing_info(billing)
    if not activity and not is_company:
        if model_has_field(ctx, "res.partner", "activity_description"):
            vals["activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_studio_activity_description"):
            vals["x_studio_activity_description"] = DEFAULT_BOLETA_ACTIVITY
        elif model_has_field(ctx, "res.partner", "x_giro"):
            vals["x_giro"] = DEFAULT_BOLETA_ACTIVITY

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner {partner_id} forzado con campos DTE: {list(vals.keys())}")


def create_partner(ctx: OdooCtx, buyer: dict, billing: dict, rut: str) -> int:
    vals = build_partner_vals_from_ml(ctx, buyer, billing, rut)
    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado para RUT {rut or 'sin_rut'}: partner_id={partner_id}")
    return partner_id


def find_or_create_partner(ctx: OdooCtx, buyer: dict, billing: dict) -> tuple[int, dict]:
    buyer_id = str(buyer.get("id", ""))
    rut = extract_rut_from_billing_info(billing)

    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        append_ml_buyer_id_to_partner(ctx, partner_id, buyer_id)
        update_partner_if_needed(ctx, partner_id, buyer, billing, rut)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = find_partner_by_buyer_id(ctx, buyer_id)
    if partner_id:
        update_partner_if_needed(ctx, partner_id, buyer, billing, rut)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = create_partner(ctx, buyer, billing, rut)
    return partner_id, read_partner(ctx, partner_id)


# =========================================================
# DECISIÓN FACTURA / BOLETA
# =========================================================

def decide_document_kind(partner_data: dict, billing: dict) -> tuple[str, str]:
    rut = normalize_rut(partner_data.get("vat") or "")
    activity = extract_activity_from_billing_info(billing)

    if partner_data.get("l10n_cl_sii_taxpayer_type") == SII_TAXPAYER_CONSUMIDOR:
        return "boleta", "Consumidor final"
    if partner_data.get("company_type") == "person":
        return "boleta", "Persona natural"
    if partner_data.get("company_type") == "company" and rut and activity:
        return "factura", "Empresa con RUT y actividad"
    return "boleta", "Fallback seguro a boleta"


# =========================================================
# DOCUMENTOS ODOO
# =========================================================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx, "account.move", "search",
        [[["ref", "=", f"ML-{order_id}"], ["state", "in", ["posted", "cancel"]]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_document_type_id(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_DOC_TYPE_FACTURA_ID", required=True)
    return to_int_env("ODOO_DOC_TYPE_BOLETA_ID", required=True)


def create_account_move(ctx: OdooCtx, order: dict, partner_id: int, document_kind: str, billing: dict, buyer: dict) -> int:
    lines = []
    for row in order.get("order_items", []):
        title = row.get("item", {}).get("title", "Producto Mercado Libre")
        qty = row.get("quantity", 1)
        unit_price_gross = float(row.get("unit_price", 0))
        unit_price_net = round(unit_price_gross / IVA_RATE, 2)
        lines.append((0, 0, {
            "name": title,
            "quantity": qty,
            "price_unit": unit_price_net,
        }))

    if not lines:
        raise Exception("La orden no tiene líneas para documentar")

    ensure_partner_dte_fields(ctx, partner_id, billing, buyer)

    vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "ref": f"ML-{order['id']}",
        "invoice_line_ids": lines,
        "l10n_latam_document_type_id": get_document_type_id(document_kind),
    }

    move_id = odoo_exec(ctx, "account.move", "create", [vals])
    odoo_exec(ctx, "account.move", "action_post", [[move_id]])
    return move_id


def create_document_in_odoo(order: dict, billing: dict) -> dict:
    ctx = odoo_connect()
    order_id = str(order["id"])

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return {"ok": True, "message": "Documento ya existe", "move_id": existing}

    buyer = order.get("buyer", {})
    partner_id, partner_data = find_or_create_partner(ctx, buyer, billing)
    document_kind, reason = decide_document_kind(partner_data, billing)

    logger.info(f"[{order_id}] Documento decidido: {document_kind} — {reason}")

    move_id = create_account_move(ctx, order, partner_id, document_kind, billing, buyer)
    logger.info(f"[{order_id}] Documento creado en Odoo: move_id={move_id}")

    return {
        "ok": True,
        "message": f"{document_kind.capitalize()} creada",
        "document_kind": document_kind,
        "reason": reason,
        "move_id": move_id,
    }


# =========================================================
# PROCESAMIENTO WEBHOOK
# =========================================================

def process_order_webhook(data: dict):
    topic = data.get("topic")
    resource = data.get("resource", "")
    logger.info(f"Webhook recibido: topic={topic} resource={resource}")

    if topic != "orders_v2":
        return

    order_id = resource.split("/")[-1]
    if not order_id:
        logger.error("No se pudo extraer order_id del webhook")
        return

    if should_skip_recent_order(order_id):
        logger.info(f"[{order_id}] Webhook duplicado reciente, se ignora")
        return

    try:
        logger.info(f"[{order_id}] Procesando orden...")
        order = get_ml_order(order_id)

        if order.get("status") != "paid":
            logger.info(f"[{order_id}] Orden no pagada. Status: {order.get('status')}")
            return

        logger.info(f"[{order_id}] Orden pagada. Creando documento...")

        billing = get_ml_billing_info(order_id)
        result = create_document_in_odoo(order, billing)
        logger.info(f"[{order_id}] Resultado final: {result}")

    except requests.HTTPError as e:
        detail = e.response.text if e.response is not None else str(e)
        logger.error(f"[{order_id}] HTTPError: {detail}", exc_info=True)
    except Exception as e:
        logger.error(f"[{order_id}] Error inesperado: {e}", exc_info=True)


# =========================================================
# WEBHOOK ML
# =========================================================

@app.post("/ml/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    background_tasks.add_task(process_order_webhook, data)
    return {"ok": True, "message": "Recibido"}
