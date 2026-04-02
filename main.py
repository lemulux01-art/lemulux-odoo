from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
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
    "ODOO_DOC_TYPE_FACTURA_ID",   # 33
    "ODOO_DOC_TYPE_BOLETA_ID",    # 39
    "ODOO_JOURNAL_FACTURA_ID",    # diario Facturas de cliente
    "ODOO_JOURNAL_BOLETA_ID",     # diario Boleta Electrónica
]

# Bandeja manual temporal
REVIEW_SALES: dict[str, dict] = {}

# Dedupe de webhook
RECENT_ORDERS: dict[str, float] = {}
RECENT_ORDERS_LOCK = Lock()
DEDUP_SECONDS = 60

TOKEN_REFRESH_INTERVAL = 5 * 60 * 60


# =========================================================
# MODELOS API
# =========================================================

class ReviewSaleUpdate(BaseModel):
    email: Optional[str] = None
    direccion: Optional[str] = None
    giro: Optional[str] = None
    tipoSugerido: Optional[str] = None   # "Factura" | "Boleta"
    estadoRevision: Optional[str] = None


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
    return (
        rut.strip()
        .upper()
        .replace(".", "")
        .replace(" ", "")
        .replace("-", "")
    )


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
    return {"status": "ok", "service": "lemulux-odoo-review"}


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
        "review_sales_count": len(REVIEW_SALES),
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


def ml_get(url: str, retries: int = 6) -> dict:
    for attempt in range(retries):
        response = requests.get(url, headers=ml_headers(), timeout=30)

        if response.status_code == 401:
            logger.warning(f"401 en {url}. Intentando refresh token...")
            if refresh_ml_token():
                response = requests.get(url, headers=ml_headers(), timeout=30)
            else:
                raise Exception("Token ML inválido o expirado")

        if response.status_code == 429:
            wait_time = min(5 * (attempt + 1), 30)
            logger.warning(f"429 en {url}. Reintento en {wait_time}s")
            time.sleep(wait_time)
            continue

        response.raise_for_status()
        return response.json()

    raise Exception(f"ML devolvió 429 demasiadas veces para {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_billing_info(order_id: str) -> dict:
    url = f"https://api.mercadolibre.com/orders/{order_id}/billing_info"
    for attempt in range(6):
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
            wait_time = min(5 * (attempt + 1), 30)
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
        recursive_find_first(billing, ["manual_giro"]),
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


def model_fields(ctx: OdooCtx, model: str) -> dict:
    return odoo_exec(ctx, model, "fields_get", [], {"attributes": ["type", "relation", "string"]})


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


def get_sale_tax_19_id(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "account.tax",
        "search",
        [[
            ["type_tax_use", "=", "sale"],
            ["amount", "=", 19],
            ["active", "=", True],
        ]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_journal_id_by_document_kind(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_JOURNAL_FACTURA_ID", required=True)
    return to_int_env("ODOO_JOURNAL_BOLETA_ID", required=True)


def read_partner(ctx: OdooCtx, partner_id: int) -> dict:
    fields = [
        "name", "vat", "email", "l10n_cl_dte_email", "comment",
        "company_type", "is_company", "country_id", "l10n_cl_sii_taxpayer_type",
    ]
    f = model_fields(ctx, "res.partner")
    for extra in [
        "activity_description",
        "l10n_cl_activity_description",
        "x_studio_activity_description",
        "x_giro",
        "giro",
        "business_activity",
    ]:
        if extra in f:
            fields.append(extra)

    if "l10n_latam_identification_type_id" in f:
        fields.append("l10n_latam_identification_type_id")

    data = odoo_exec(ctx, "res.partner", "read", [[partner_id], fields])
    return data[0] if data else {}


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut_norm = normalize_rut(rut)
    if not rut_norm:
        return None

    ids = odoo_exec(
        ctx,
        "res.partner",
        "search",
        [[["vat", "!=", False]]],
        {"limit": 500},
    )
    if not ids:
        return None

    partners = odoo_exec(ctx, "res.partner", "read", [ids, ["vat"]])

    for partner in partners:
        if normalize_rut(partner.get("vat") or "") == rut_norm:
            return partner["id"]

    return None


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


# =========================================================
# GIRO / ACTIVITY
# =========================================================

def get_partner_activity_candidates(ctx: OdooCtx) -> list[tuple[str, dict]]:
    fields = model_fields(ctx, "res.partner")
    candidates = []

    preferred = [
        "activity_description",
        "l10n_cl_activity_description",
        "x_studio_activity_description",
        "x_giro",
        "giro",
        "business_activity",
    ]

    for name in preferred:
        if name in fields:
            candidates.append((name, fields[name]))

    for name, meta in fields.items():
        if any(name == x for x, _ in candidates):
            continue
        label = (meta.get("string") or "").lower()
        n = name.lower()
        if "giro" in n or "giro" in label:
            candidates.append((name, meta))
        elif "activity" in n or "actividad" in label:
            if meta.get("type") in ("char", "text", "many2one"):
                candidates.append((name, meta))

    return candidates


def get_default_activity_value_for_field(ctx: OdooCtx, field_meta: dict) -> Any:
    ftype = field_meta.get("type")

    if ftype in ("char", "text"):
        return DEFAULT_BOLETA_ACTIVITY

    if ftype == "many2one":
        relation = field_meta.get("relation")
        if not relation:
            return None

        try:
            ids = odoo_exec(
                ctx, relation, "search",
                [[["name", "ilike", "boleta"]]],
                {"limit": 1},
            )
            if ids:
                return ids[0]
        except Exception:
            pass

        try:
            ids = odoo_exec(ctx, relation, "search", [[]], {"limit": 1})
            if ids:
                return ids[0]
        except Exception:
            pass

        try:
            rel_fields = model_fields(ctx, relation)
            if "name" in rel_fields:
                return odoo_exec(ctx, relation, "create", [{"name": DEFAULT_BOLETA_ACTIVITY}])
        except Exception:
            pass

    return None


def apply_partner_activity_values(vals: dict, ctx: OdooCtx, activity_text: str, current_partner: Optional[dict] = None):
    candidates = get_partner_activity_candidates(ctx)

    if not candidates:
        logger.warning("No se detectaron campos candidatos de giro/actividad en res.partner")
        return

    for field_name, meta in candidates:
        current_val = current_partner.get(field_name) if current_partner else None
        if current_val:
            continue

        ftype = meta.get("type")
        if ftype in ("char", "text"):
            vals[field_name] = activity_text
        elif ftype == "many2one":
            default_val = get_default_activity_value_for_field(ctx, meta)
            if default_val:
                vals[field_name] = default_val


def has_any_partner_activity(current_partner: dict, ctx: OdooCtx) -> bool:
    for field_name, _meta in get_partner_activity_candidates(ctx):
        val = current_partner.get(field_name)
        if val:
            return True
    return False


def build_partner_vals_from_ml(ctx: OdooCtx, buyer: dict, billing: dict, rut: str, document_kind: str) -> dict:
    chile_country_id = get_chile_country_id(ctx)
    rut_identification_type_id = get_rut_identification_type_id(ctx)
    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    vals = {
        "name": extract_name_from_billing_info(billing, buyer),
        "vat": rut or False,
        "email": billing.get("manual_email") or ML_DEFAULT_EMAIL,
        "l10n_cl_dte_email": billing.get("manual_email") or ML_DEFAULT_EMAIL,
        "customer_rank": 1,
        "company_type": "company" if is_company else "person",
        "is_company": is_company,
        "country_id": chile_country_id,
        "l10n_cl_sii_taxpayer_type": sii_taxpayer_type,
    }

    f = model_fields(ctx, "res.partner")
    if rut and rut_identification_type_id and "l10n_latam_identification_type_id" in f:
        vals["l10n_latam_identification_type_id"] = rut_identification_type_id

    activity = extract_activity_from_billing_info(billing)

    # Solo boleta puede usar giro forzado
    if document_kind == "boleta":
        activity = activity or DEFAULT_BOLETA_ACTIVITY

    if activity:
        apply_partner_activity_values(vals, ctx, activity)

    return vals


def update_partner_if_needed(ctx: OdooCtx, partner_id: int, buyer: dict, billing: dict, rut: str, document_kind: str):
    current = read_partner(ctx, partner_id)
    vals = {}

    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    if not current.get("vat") and rut:
        vals["vat"] = rut

    if not current.get("l10n_cl_sii_taxpayer_type"):
        vals["l10n_cl_sii_taxpayer_type"] = sii_taxpayer_type

    if not current.get("email"):
        vals["email"] = billing.get("manual_email") or ML_DEFAULT_EMAIL

    if not current.get("l10n_cl_dte_email"):
        vals["l10n_cl_dte_email"] = billing.get("manual_email") or ML_DEFAULT_EMAIL

    if not current.get("country_id"):
        vals["country_id"] = get_chile_country_id(ctx)

    if not has_any_partner_activity(current, ctx):
        activity = extract_activity_from_billing_info(billing)
        if document_kind == "boleta":
            activity = activity or DEFAULT_BOLETA_ACTIVITY
        if activity:
            apply_partner_activity_values(vals, ctx, activity, current)

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner {partner_id} actualizado con campos DTE/giro: {list(vals.keys())}")


def ensure_partner_dte_fields(ctx: OdooCtx, partner_id: int, billing: dict, buyer: dict, document_kind: str):
    current = read_partner(ctx, partner_id)
    vals = {}

    is_company = should_be_company_from_ml(billing, buyer)
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    if not current.get("email"):
        vals["email"] = billing.get("manual_email") or ML_DEFAULT_EMAIL

    if not current.get("l10n_cl_dte_email"):
        vals["l10n_cl_dte_email"] = billing.get("manual_email") or ML_DEFAULT_EMAIL

    if not current.get("country_id"):
        vals["country_id"] = get_chile_country_id(ctx)

    if not current.get("l10n_cl_sii_taxpayer_type"):
        vals["l10n_cl_sii_taxpayer_type"] = sii_taxpayer_type

    if not has_any_partner_activity(current, ctx):
        activity = extract_activity_from_billing_info(billing)
        if document_kind == "boleta":
            activity = activity or DEFAULT_BOLETA_ACTIVITY
        if activity:
            apply_partner_activity_values(vals, ctx, activity, current)

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])
        logger.info(f"Partner {partner_id} forzado con campos DTE/giro: {list(vals.keys())}")

    refreshed = read_partner(ctx, partner_id)

    # Solo factura exige actividad real; boleta permite giro forzado
    if document_kind == "factura" and not has_any_partner_activity(refreshed, ctx):
        raise Exception(
            f"No se pudo asignar actividad/giro real al partner {partner_id} para factura."
        )


def create_partner(ctx: OdooCtx, buyer: dict, billing: dict, rut: str, document_kind: str) -> int:
    vals = build_partner_vals_from_ml(ctx, buyer, billing, rut, document_kind)
    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado para RUT {rut or 'sin_rut'}: partner_id={partner_id}")
    return partner_id


def find_or_create_partner(ctx: OdooCtx, buyer: dict, billing: dict, document_kind: str) -> tuple[int, dict]:
    buyer_id = str(buyer.get("id", ""))
    rut = extract_rut_from_billing_info(billing)

    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        append_ml_buyer_id_to_partner(ctx, partner_id, buyer_id)
        update_partner_if_needed(ctx, partner_id, buyer, billing, rut, document_kind)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = find_partner_by_buyer_id(ctx, buyer_id)
    if partner_id:
        update_partner_if_needed(ctx, partner_id, buyer, billing, rut, document_kind)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = create_partner(ctx, buyer, billing, rut, document_kind)
    return partner_id, read_partner(ctx, partner_id)


# =========================================================
# DECISIÓN FACTURA / BOLETA
# =========================================================

def decide_document_kind_from_billing_and_override(billing: dict, buyer: dict, override: Optional[str] = None) -> tuple[str, str]:
    if override:
        override_clean = override.strip().lower()
        if override_clean == "factura":
            return "factura", "Selección manual"
        if override_clean == "boleta":
            return "boleta", "Selección manual"

    rut = extract_rut_from_billing_info(billing)
    activity = extract_activity_from_billing_info(billing)

    if ml_indicates_final_consumer(billing):
        return "boleta", "Consumidor final"
    if should_be_company_from_ml(billing, buyer) and rut and activity:
        return "factura", "Empresa con RUT y actividad"
    return "boleta", "Fallback seguro a boleta"


# =========================================================
# DOCUMENTOS ODOO
# =========================================================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx, "account.move", "search",
        [[["ref", "=", f"ML-{order_id}"], ["state", "in", ["draft", "posted", "cancel"]]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_document_type_id(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_DOC_TYPE_FACTURA_ID", required=True)
    return to_int_env("ODOO_DOC_TYPE_BOLETA_ID", required=True)


def create_account_move(ctx: OdooCtx, order: dict, partner_id: int, document_kind: str, billing: dict, buyer: dict) -> int:
    lines = []
    tax_19_id = get_sale_tax_19_id(ctx)
    journal_id = get_journal_id_by_document_kind(document_kind)

    for row in order.get("order_items", []):
        title = row.get("item", {}).get("title", "Producto Mercado Libre")
        qty = row.get("quantity", 1)
        unit_price_gross = float(row.get("unit_price", 0))
        unit_price_net = round(unit_price_gross / IVA_RATE, 2)

        line_vals = {
            "name": title,
            "quantity": qty,
            "price_unit": unit_price_net,
        }

        if tax_19_id:
            line_vals["tax_ids"] = [(6, 0, [tax_19_id])]

        lines.append((0, 0, line_vals))

    if not lines:
        raise Exception("La orden no tiene líneas para documentar")

    ensure_partner_dte_fields(ctx, partner_id, billing, buyer, document_kind)

    vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "ref": f"ML-{order['id']}",
        "invoice_line_ids": lines,
        "l10n_latam_document_type_id": get_document_type_id(document_kind),
        "journal_id": journal_id,
    }

    move_id = odoo_exec(ctx, "account.move", "create", [vals])
    odoo_exec(ctx, "account.move", "action_post", [[move_id]])
    return move_id


def create_document_in_odoo(order: dict, billing: dict, tipo_sugerido: Optional[str] = None) -> dict:
    ctx = odoo_connect()
    order_id = str(order["id"])

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return {"ok": True, "message": "Documento ya existe", "move_id": existing}

    buyer = order.get("buyer", {})
    document_kind, reason = decide_document_kind_from_billing_and_override(
        billing=billing,
        buyer=buyer,
        override=tipo_sugerido,
    )

    partner_id, partner_data = find_or_create_partner(ctx, buyer, billing, document_kind)

    logger.info(f"[{order_id}] Documento decidido: {document_kind} — {reason}")

    move_id = create_account_move(ctx, order, partner_id, document_kind, billing, buyer)
    logger.info(f"[{order_id}] Documento creado en Odoo: move_id={move_id}")

    return {
        "ok": True,
        "message": f"{document_kind.capitalize()} creada",
        "document_kind": document_kind,
        "reason": reason,
        "move_id": move_id,
        "partner_id": partner_id,
        "partner_name": partner_data.get("name"),
    }


# =========================================================
# BANDEJA DE REVISIÓN
# =========================================================

def save_sale_for_review(order: dict, billing: dict):
    order_id = str(order["id"])
    buyer = order.get("buyer", {})

    document_kind, _reason = decide_document_kind_from_billing_and_override(billing, buyer, None)

    suggested_label = "Factura" if document_kind == "factura" else "Boleta"

    giro = extract_activity_from_billing_info(billing)
    if document_kind == "boleta":
        giro = giro or DEFAULT_BOLETA_ACTIVITY

    REVIEW_SALES[order_id] = {
        "id": order_id,
        "fecha": order.get("date_created", ""),
        "estadoML": order.get("status", ""),
        "estadoRevision": "Pendiente revisión",
        "tipoSugerido": suggested_label,
        "cliente": extract_name_from_billing_info(billing, buyer),
        "rut": extract_rut_from_billing_info(billing),
        "email": ML_DEFAULT_EMAIL,
        "telefono": "",
        "direccion": "Chile",
        "giro": giro,
        "errores": [],
        "items": [
            {
                "nombre": row.get("item", {}).get("title", "Producto Mercado Libre"),
                "cantidad": row.get("quantity", 1),
                "total": f"${int(float(row.get('unit_price', 0))):,}".replace(",", "."),
            }
            for row in order.get("order_items", [])
        ],
    }


# =========================================================
# WEBHOOK ML
# NO ENVÍA A ODOO. SOLO GUARDA EN REVISIÓN.
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

        billing = get_ml_billing_info(order_id)
        save_sale_for_review(order, billing)
        logger.info(f"[{order_id}] Venta guardada en bandeja de revisión")

    except requests.HTTPError as e:
        detail = e.response.text if e.response is not None else str(e)
        logger.error(f"[{order_id}] HTTPError: {detail}", exc_info=True)
    except Exception as e:
        logger.error(f"[{order_id}] Error inesperado: {e}", exc_info=True)


@app.post("/ml/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    thread = threading.Thread(target=process_order_webhook, args=(data,), daemon=True)
    thread.start()

    return {"ok": True, "message": "Recibido"}


# =========================================================
# ENDPOINTS PANEL MANUAL
# =========================================================

@app.get("/review/sales")
def get_review_sales():
    return {"sales": list(REVIEW_SALES.values())}


@app.get("/review/sales/{sale_id}")
def get_review_sale_detail(sale_id: str):
    sale = REVIEW_SALES.get(sale_id)
    if not sale:
        raise HTTPException(status_code=404, detail="Venta no encontrada")
    return sale


@app.patch("/review/sales/{sale_id}")
def patch_review_sale(sale_id: str, payload: ReviewSaleUpdate):
    sale = REVIEW_SALES.get(sale_id)
    if not sale:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    data = payload.dict(exclude_unset=True)
    sale.update(data)
    REVIEW_SALES[sale_id] = sale

    return {"ok": True, "message": "Venta actualizada", "sale": sale}


@app.post("/review/sales/{sale_id}/send-to-odoo")
def send_review_sale_to_odoo(sale_id: str):
    sale = REVIEW_SALES.get(sale_id)
    if not sale:
        raise HTTPException(status_code=404, detail="Venta no encontrada")

    order = get_ml_order(sale_id)
    billing = get_ml_billing_info(sale_id)

    # Aplicar correcciones manuales de la bandeja
    if sale.get("email"):
        billing["manual_email"] = sale["email"]

    if sale.get("giro"):
        billing["manual_giro"] = sale["giro"]

    result = create_document_in_odoo(
        order=order,
        billing=billing,
        tipo_sugerido=sale.get("tipoSugerido"),
    )

    sale["estadoRevision"] = "Enviada a Odoo"
    REVIEW_SALES[sale_id] = sale

    return {
        "ok": True,
        "message": "Venta enviada a Odoo",
        "result": result,
    }
