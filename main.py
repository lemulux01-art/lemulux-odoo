from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import os
import logging
import requests
import xmlrpc.client
from typing import Optional, Any
from dataclasses import dataclass
import time
from threading import Lock


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lemulux")

app = FastAPI()


# =========================================================
# Constantes
# =========================================================

IVA_RATE = 1.19
ML_DEFAULT_EMAIL = "odoo@lemulux.com"
RAILWAY_API_URL = "https://backboard.railway.com/graphql/v2"

# Valores de l10n_cl_sii_taxpayer_type en Odoo 18 Chile
# 1 = 1a Categoría (empresa contribuyente → factura)
# 3 = IVSC
# 4 = Consumidor Final (persona natural → boleta)
SII_TAXPAYER_EMPRESA   = "1"
SII_TAXPAYER_CONSUMIDOR = "4"

# Status de envío ML Chile (Cross Docking)
SHIPMENT_STATUSES_DOCUMENTABLES   = {"shipped", "ready_to_ship"}
SHIPMENT_SUBSTATUSES_DOCUMENTABLES = {"picked_up", "authorized_by_carrier", "in_hub"}

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
    "RAILWAY_API_TOKEN",
    "RAILWAY_PROJECT_ID",
    "RAILWAY_ENVIRONMENT_ID",
    "RAILWAY_SERVICE_ID",
]

# Anti-duplicados webhook en memoria
RECENT_ORDERS: dict = {}
RECENT_ORDERS_LOCK = Lock()
DEDUP_SECONDS = 60


# =========================================================
# Helpers generales
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


def normalize_rut(rut: str) -> str:
    if not rut:
        return ""
    return rut.strip().upper().replace(".", "").replace(" ", "")


def to_int_env(name: str, required: bool = True) -> Optional[int]:
    raw = get_env(name, required=required)
    if not raw:
        return None
    return int(raw)


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
# Validación al arrancar
# =========================================================

@app.on_event("startup")
async def validate_env():
    missing = [k for k in REQUIRED_ENV_VARS if not os.getenv(k, "").strip()]
    if missing:
        raise RuntimeError(f"❌ Variables de entorno faltantes al iniciar: {missing}")
    logger.info("✅ Variables de entorno cargadas correctamente")


# =========================================================
# Endpoints base
# =========================================================

@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


# =========================================================
# Diagnóstico completo
# =========================================================

@app.get("/ml/diagnostico")
def diagnostico():
    resultado = {
        "servicio": "ok",
        "variables": {},
        "mercadolibre": {},
        "odoo": {},
        "documentos": {},
    }

    # ── 1. Variables ─────────────────────────────────────
    faltantes = [k for k in REQUIRED_ENV_VARS if not os.getenv(k, "").strip()]
    placeholders = [k for k in ["ML_ACCESS_TOKEN", "ML_REFRESH_TOKEN"] if os.getenv(k, "") == "placeholder"]
    resultado["variables"] = {
        "ok": len(faltantes) == 0 and len(placeholders) == 0,
        "faltantes": faltantes,
        "placeholders": placeholders,
    }

    # ── 2. Mercado Libre ─────────────────────────────────
    try:
        token = get_env("ML_ACCESS_TOKEN")
        response = requests.get(
            "https://api.mercadolibre.com/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            resultado["mercadolibre"] = {
                "ok": True,
                "user_id": data.get("id"),
                "nickname": data.get("nickname"),
                "token_valido": True,
            }
        elif response.status_code == 401:
            resultado["mercadolibre"] = {"ok": False, "error": "Token expirado (401)", "token_valido": False}
        else:
            resultado["mercadolibre"] = {"ok": False, "error": f"Error {response.status_code}", "token_valido": False}
    except Exception as e:
        resultado["mercadolibre"] = {"ok": False, "error": str(e)}

    # ── 3. Odoo ──────────────────────────────────────────
    uid = None
    odoo_db = None
    odoo_api_key = None
    try:
        odoo_url = get_env("ODOO_URL")
        odoo_db = get_env("ODOO_DB")
        odoo_user = get_env("ODOO_USER")
        odoo_api_key = get_env("ODOO_API_KEY")
        common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
        uid = common.authenticate(odoo_db, odoo_user, odoo_api_key, {})
        if uid:
            resultado["odoo"] = {"ok": True, "uid": uid, "url": odoo_url, "db": odoo_db}
        else:
            resultado["odoo"] = {"ok": False, "error": "Autenticación fallida"}
    except Exception as e:
        resultado["odoo"] = {"ok": False, "error": str(e)}

    # ── 4. Tipos de documento ────────────────────────────
    try:
        factura_id = to_int_env("ODOO_DOC_TYPE_FACTURA_ID")
        boleta_id = to_int_env("ODOO_DOC_TYPE_BOLETA_ID")
        if resultado["odoo"].get("ok") and uid:
            models = xmlrpc.client.ServerProxy(f"{get_env('ODOO_URL')}/xmlrpc/2/object")
            def check_doc(doc_id, nombre):
                ids = models.execute_kw(odoo_db, uid, odoo_api_key,
                    "l10n_latam.document.type", "search", [[["id", "=", doc_id]]], {"limit": 1})
                return {"id": doc_id, "existe": bool(ids), "nombre": nombre}
            resultado["documentos"] = {
                "ok": True,
                "factura": check_doc(factura_id, "Factura Electrónica"),
                "boleta": check_doc(boleta_id, "Boleta Electrónica"),
            }
        else:
            resultado["documentos"] = {"ok": False, "error": "Odoo no conectado"}
    except Exception as e:
        resultado["documentos"] = {"ok": False, "error": str(e)}

    todo_ok = all([
        resultado["variables"]["ok"],
        resultado["mercadolibre"].get("ok", False),
        resultado["odoo"].get("ok", False),
        resultado["documentos"].get("ok", False),
    ])
    resultado["estado_general"] = "✅ Todo operativo" if todo_ok else "⚠️ Hay problemas — revisa los detalles"
    return resultado


# =========================================================
# Persistencia de tokens en Railway
# =========================================================

def persist_tokens_to_railway(access_token: str, refresh_token: str):
    railway_api_token = os.getenv("RAILWAY_API_TOKEN", "")
    project_id = os.getenv("RAILWAY_PROJECT_ID", "")
    environment_id = os.getenv("RAILWAY_ENVIRONMENT_ID", "")
    service_id = os.getenv("RAILWAY_SERVICE_ID", "")

    if not all([railway_api_token, project_id, environment_id, service_id]):
        logger.warning("⚠️ Variables de Railway no configuradas — tokens solo en memoria")
        return

    mutation = """
    mutation variableCollectionUpsert($input: VariableCollectionUpsertInput!) {
      variableCollectionUpsert(input: $input)
    }
    """
    payload = {
        "query": mutation,
        "variables": {
            "input": {
                "projectId": project_id,
                "environmentId": environment_id,
                "serviceId": service_id,
                "variables": {
                    "ML_ACCESS_TOKEN": access_token,
                    "ML_REFRESH_TOKEN": refresh_token,
                },
            }
        },
    }
    try:
        response = requests.post(
            RAILWAY_API_URL,
            json=payload,
            headers={"Authorization": f"Bearer {railway_api_token}", "Content-Type": "application/json"},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            logger.error(f"❌ Railway API error: {data['errors']}")
        else:
            logger.info("✅ Tokens persistidos en Railway")
    except Exception as e:
        logger.error(f"❌ Error persistiendo tokens en Railway: {e}", exc_info=True)


# =========================================================
# Renovación automática del token ML
# =========================================================

def refresh_ml_token() -> bool:
    try:
        payload = {
            "grant_type": "refresh_token",
            "client_id": get_env("ML_CLIENT_ID"),
            "client_secret": get_env("ML_CLIENT_SECRET"),
            "refresh_token": get_env("ML_REFRESH_TOKEN"),
        }
        response = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        new_access_token = data.get("access_token")
        new_refresh_token = data.get("refresh_token") or get_env("ML_REFRESH_TOKEN")

        if not new_access_token:
            logger.error("Token refresh: respuesta sin access_token")
            return False

        os.environ["ML_ACCESS_TOKEN"] = new_access_token
        os.environ["ML_REFRESH_TOKEN"] = new_refresh_token
        persist_tokens_to_railway(new_access_token, new_refresh_token)
        logger.info("✅ Token ML renovado y persistido")
        return True
    except Exception as e:
        logger.error(f"❌ Error renovando token ML: {e}", exc_info=True)
        return False


# =========================================================
# Mercado Libre API
# =========================================================

def ml_headers() -> dict:
    return {"Authorization": f"Bearer {get_env('ML_ACCESS_TOKEN')}"}


def ml_get(url: str, retries: int = 4, backoff: int = 2) -> dict:
    last_error = None
    for attempt in range(retries):
        response = requests.get(url, headers=ml_headers(), timeout=30)
        if response.status_code == 429:
            wait = backoff * (attempt + 1)
            logger.warning(f"429 en {url}. Reintento en {wait}s")
            time.sleep(wait)
            last_error = f"429 Too Many Requests: {url}"
            continue
        if response.status_code == 401:
            logger.warning(f"Token expirado en {url}, renovando...")
            if not refresh_ml_token():
                raise Exception("No se pudo renovar el token ML")
            response = requests.get(url, headers=ml_headers(), timeout=30)
        response.raise_for_status()
        return response.json()
    raise Exception(last_error or f"No se pudo consultar ML: {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_shipment(shipment_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/shipments/{shipment_id}")


def get_ml_billing_info(order_id: str) -> dict:
    url = f"https://api.mercadolibre.com/orders/{order_id}/billing_info"
    last_error = None
    for attempt in range(4):
        response = requests.get(url, headers=ml_headers(), timeout=30)
        if response.status_code == 404:
            return {}
        if response.status_code == 429:
            wait = 2 * (attempt + 1)
            logger.warning(f"429 en billing_info {order_id}. Reintento en {wait}s")
            time.sleep(wait)
            last_error = f"429 en billing_info {order_id}"
            continue
        if response.status_code == 401:
            logger.warning("Token expirado en billing_info, renovando...")
            if not refresh_ml_token():
                raise Exception("No se pudo renovar el token ML")
            response = requests.get(url, headers=ml_headers(), timeout=30)
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        return response.json()
    raise Exception(last_error or f"No se pudo obtener billing_info para {order_id}")


# =========================================================
# OAuth Mercado Libre
# =========================================================

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
        response = requests.post("https://api.mercadolibre.com/oauth/token", data=payload, timeout=30)
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
            if access_token and refresh_token:
                persist_tokens_to_railway(access_token, refresh_token)
            logger.info("✅ Tokens OAuth guardados")

        return JSONResponse(status_code=response.status_code, content={"status_code": response.status_code, "response": data})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/ml/refresh-token")
def manual_refresh_token():
    success = refresh_ml_token()
    if success:
        return {"ok": True, "message": "Token renovado y persistido en Railway"}
    return JSONResponse(status_code=500, content={"ok": False, "message": "No se pudo renovar el token"})


# =========================================================
# Extracción de datos de billing ML
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


def extract_name_from_billing(billing: dict, buyer: dict) -> str:
    return first_non_empty(
        recursive_find_first(billing, ["name"]),
        recursive_find_first(billing, ["social_reason"]),
        recursive_find_first(billing, ["razon_social"]),
        recursive_find_first(billing, ["business_name"]),
        buyer.get("first_name"),
        buyer.get("nickname"),
        "Cliente Mercado Libre",
    )


def extract_activity_from_billing(billing: dict) -> str:
    return first_non_empty(
        recursive_find_first(billing, ["business_activity"]),
        recursive_find_first(billing, ["activity"]),
        recursive_find_first(billing, ["economic_activity"]),
        recursive_find_first(billing, ["taxpayer_activity"]),
        recursive_find_first(billing, ["activity_description"]),
        recursive_find_first(billing, ["giro"]),
        recursive_find_first(billing, ["description_of_activity"]),
    )


def extract_taxpayer_type_from_billing(billing: dict) -> str:
    return first_non_empty(
        recursive_find_first(billing, ["taxpayer_type"]),
        recursive_find_first(billing, ["taxpayer_kind"]),
        recursive_find_first(billing, ["taxpayer_category"]),
        recursive_find_first(billing, ["contributor_type"]),
        recursive_find_first(billing, ["tipo_contribuyente"]),
        recursive_find_first(billing, ["description"]),
    ).strip().lower()


def billing_is_final_consumer(billing: dict) -> bool:
    t = extract_taxpayer_type_from_billing(billing)
    return any(x in t for x in ["consumidor final", "final consumer", "consumer final"])


def billing_looks_like_company(billing: dict, buyer: dict) -> bool:
    name = extract_name_from_billing(billing, buyer).upper()
    markers = ["SPA", "EIRL", "LTDA", "S.A", "S.A.", "SOCIEDAD", "COMERCIAL",
               "COMERCIALIZADORA", "CONSTRUCTORA", "TRANSPORTES", "INVERSIONES",
               "IMPORTADORA", "EXPORTADORA"]
    return any(m in name for m in markers)


def decide_should_be_company(billing: dict, buyer: dict) -> bool:
    """¿Debe el partner clasificarse como empresa?"""
    if billing_is_final_consumer(billing):
        return False
    if extract_activity_from_billing(billing):
        return True
    if billing_looks_like_company(billing, buyer):
        return True
    return False


# =========================================================
# Odoo: contexto único por request
# =========================================================

@dataclass
class OdooCtx:
    db: str
    api_key: str
    uid: int
    models: Any


def odoo_connect() -> OdooCtx:
    odoo_url = get_env("ODOO_URL")
    common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
    uid = common.authenticate(get_env("ODOO_DB"), get_env("ODOO_USER"), get_env("ODOO_API_KEY"), {})
    if not uid:
        raise Exception("No se pudo autenticar en Odoo")
    models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")
    return OdooCtx(db=get_env("ODOO_DB"), api_key=get_env("ODOO_API_KEY"), uid=uid, models=models)


def odoo_exec(ctx: OdooCtx, model: str, method: str, args: list, kwargs: dict = None) -> Any:
    return ctx.models.execute_kw(ctx.db, ctx.uid, ctx.api_key, model, method, args, kwargs or {})


# =========================================================
# Helpers Odoo Chile
# =========================================================

def get_chile_country_id(ctx: OdooCtx) -> int:
    ids = odoo_exec(ctx, "res.country", "search", [[["code", "=", "CL"]]], {"limit": 1})
    if not ids:
        raise Exception("No se encontró Chile en Odoo")
    return ids[0]


def get_rut_identification_type_id(ctx: OdooCtx) -> Optional[int]:
    """Busca el tipo de identificación RUT en l10n_latam.identification.type."""
    for domain in [
        [[["name", "ilike", "RUT"], ["country_id.code", "=", "CL"]]],
        [[["name", "ilike", "RUT"]]],
    ]:
        ids = odoo_exec(ctx, "l10n_latam.identification.type", "search", domain, {"limit": 1})
        if ids:
            return ids[0]
    return None


def get_consumidor_final_partner_id(ctx: OdooCtx) -> Optional[int]:
    """
    Busca el partner 'Consumidor Final' que Odoo Chile crea por defecto.
    Este partner se usa para boletas cuando no hay RUT del comprador.
    """
    for domain in [
        [[["name", "ilike", "Consumidor Final"], ["vat", "!=", False]]],
        [[["name", "ilike", "Consumidor Final"]]],
        [[["vat", "=", "66666666-6"]]],  # RUT genérico que usa Odoo Chile
    ]:
        ids = odoo_exec(ctx, "res.partner", "search", domain, {"limit": 1})
        if ids:
            return ids[0]
    return None


# =========================================================
# Partners / Clientes
# =========================================================

def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
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


def read_partner(ctx: OdooCtx, partner_id: int) -> dict:
    data = odoo_exec(
        ctx, "res.partner", "read",
        [[partner_id], ["name", "vat", "email", "comment", "company_type",
                        "is_company", "country_id", "l10n_cl_sii_taxpayer_type"]],
    )
    return data[0] if data else {}


def append_ml_buyer_id(ctx: OdooCtx, partner_id: int, buyer_id: str):
    if not buyer_id:
        return
    current = read_partner(ctx, partner_id)
    comment = current.get("comment") or ""
    marker = f"ML_BUYER_ID:{buyer_id}"
    if marker in comment:
        return
    odoo_exec(ctx, "res.partner", "write", [[partner_id], {"comment": f"{comment}\n{marker}".strip()}])


def build_partner_vals(ctx: OdooCtx, buyer: dict, billing: dict, rut: str) -> dict:
    """
    Construye los campos del partner según localización Chile Odoo 18.
    Campos obligatorios para DTE:
    - country_id = Chile
    - l10n_cl_sii_taxpayer_type = "1" (empresa) o "4" (consumidor final)
    - l10n_latam_identification_type_id = RUT (si tiene RUT)
    - vat = RUT
    """
    buyer_id = str(buyer.get("id", ""))
    name = extract_name_from_billing(billing, buyer)
    is_company = decide_should_be_company(billing, buyer)
    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_identification_type_id(ctx)

    # Tipo de contribuyente SII
    sii_taxpayer_type = SII_TAXPAYER_EMPRESA if is_company else SII_TAXPAYER_CONSUMIDOR

    vals = {
        "name": name,
        "vat": rut or False,
        "email": ML_DEFAULT_EMAIL,
        "comment": f"ML_BUYER_ID:{buyer_id}" if buyer_id else False,
        "customer_rank": 1,
        "company_type": "company" if is_company else "person",
        "is_company": is_company,
        "country_id": chile_id,
        "l10n_cl_sii_taxpayer_type": sii_taxpayer_type,
    }

    # Tipo de identificación RUT (necesario para que Odoo genere el DTE correctamente)
    if rut and rut_type_id:
        vals["l10n_latam_identification_type_id"] = rut_type_id

    return vals


def create_or_update_partner(ctx: OdooCtx, partner_id: int, buyer: dict, billing: dict, rut: str):
    """Actualiza campos faltantes o incorrectos en un partner existente."""
    current = read_partner(ctx, partner_id)
    desired = build_partner_vals(ctx, buyer, billing, rut)
    vals = {}

    for key, value in desired.items():
        current_val = current.get(key)
        # Normalizar many2one que viene como [id, name]
        if isinstance(current_val, list) and current_val:
            current_val = current_val[0]
        # Solo actualizar si está vacío o incorrecto
        if current_val in (False, None, "", []) or (key == "l10n_cl_sii_taxpayer_type" and current_val != value):
            vals[key] = value

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])


def find_or_create_partner(ctx: OdooCtx, buyer: dict, billing: dict) -> tuple[int, dict]:
    buyer_id = str(buyer.get("id", ""))
    rut = extract_rut_from_billing_info(billing)

    # 1. Buscar por RUT
    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        logger.info(f"Partner encontrado por RUT {rut}: id={partner_id}")
        append_ml_buyer_id(ctx, partner_id, buyer_id)
        create_or_update_partner(ctx, partner_id, buyer, billing, rut)
        return partner_id, read_partner(ctx, partner_id)

    # 2. Buscar por ML_BUYER_ID
    partner_id = find_partner_by_buyer_id(ctx, buyer_id)
    if partner_id:
        logger.info(f"Partner encontrado por ML_BUYER_ID: id={partner_id}")
        create_or_update_partner(ctx, partner_id, buyer, billing, rut)
        return partner_id, read_partner(ctx, partner_id)

    # 3. Crear nuevo
    vals = build_partner_vals(ctx, buyer, billing, rut)
    partner_id = odoo_exec(ctx, "res.partner", "create", [vals])
    logger.info(f"Partner creado: id={partner_id}")
    return partner_id, read_partner(ctx, partner_id)


# =========================================================
# Decisión factura / boleta
# =========================================================

def decide_document_kind(partner_data: dict, billing: dict) -> tuple[str, str]:
    """
    Reglas para Odoo 18 Chile:
    - FACTURA (33): empresa con RUT + actividad económica + no consumidor final
    - BOLETA  (39): todos los demás casos
    """
    if billing_is_final_consumer(billing):
        return "boleta", "ML indica consumidor final"

    rut = normalize_rut(partner_data.get("vat") or "")
    activity = extract_activity_from_billing(billing)
    is_company = partner_data.get("company_type") == "company"
    sii_type = partner_data.get("l10n_cl_sii_taxpayer_type") or ""

    # Factura solo si: empresa + RUT + actividad económica + tipo contribuyente 1a categoría
    if is_company and rut and activity and sii_type == SII_TAXPAYER_EMPRESA:
        return "factura", "Empresa con RUT, actividad económica y tipo contribuyente 1a categoría"

    return "boleta", "Fallback seguro a boleta"


# =========================================================
# Documentos en Odoo
# =========================================================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    """Solo retorna documentos ya confirmados (ignora borradores rotos)."""
    ids = odoo_exec(
        ctx, "account.move", "search",
        [[["ref", "=", f"ML-{order_id}"], ["state", "in", ["posted", "cancel"]]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_document_type_id(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_DOC_TYPE_FACTURA_ID")
    return to_int_env("ODOO_DOC_TYPE_BOLETA_ID")


def get_partner_for_document(ctx: OdooCtx, partner_id: int, document_kind: str) -> int:
    """
    Para boleta: si el partner no tiene RUT, usar el partner 'Consumidor Final'
    de Odoo Chile en vez del partner creado. Esto evita errores de validación DTE.
    """
    if document_kind == "boleta":
        partner_data = read_partner(ctx, partner_id)
        rut = normalize_rut(partner_data.get("vat") or "")
        if not rut:
            cf_id = get_consumidor_final_partner_id(ctx)
            if cf_id:
                logger.info(f"Boleta sin RUT → usando partner Consumidor Final: id={cf_id}")
                return cf_id
    return partner_id


def create_account_move(ctx: OdooCtx, order: dict, partner_id: int, document_kind: str) -> int:
    lines = []
    for row in order.get("order_items", []):
        title = row.get("item", {}).get("title", "Producto Mercado Libre")
        qty = row.get("quantity", 1)
        unit_price_gross = float(row.get("unit_price", 0))
        unit_price_net = round(unit_price_gross / IVA_RATE, 2)
        lines.append((0, 0, {"name": title, "quantity": qty, "price_unit": unit_price_net}))

    if not lines:
        raise Exception("La orden no tiene líneas para documentar")

    # Para boleta sin RUT → usar Consumidor Final
    effective_partner_id = get_partner_for_document(ctx, partner_id, document_kind)

    vals = {
        "move_type": "out_invoice",
        "partner_id": effective_partner_id,
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

    buyer = order.get("buyer", {}) or {}
    partner_id, partner_data = find_or_create_partner(ctx, buyer, billing)

    document_kind, reason = decide_document_kind(partner_data, billing)
    logger.info(f"[{order_id}] Documento: {document_kind} — {reason}")

    move_id = create_account_move(ctx, order, partner_id, document_kind)
    logger.info(f"[{order_id}] ✅ Creado en Odoo: move_id={move_id} tipo={document_kind}")

    return {"ok": True, "move_id": move_id, "document_kind": document_kind, "reason": reason}


# =========================================================
# Lógica de envío documentable (ML Chile Cross Docking)
# =========================================================

def shipment_es_documentable(shipment: dict) -> bool:
    status = shipment.get("status", "")
    substatus = shipment.get("substatus", "")
    if status == "shipped":
        return True
    if status == "ready_to_ship" and substatus in SHIPMENT_SUBSTATUSES_DOCUMENTABLES:
        return True
    return False


# =========================================================
# Procesamiento de orden (background)
# =========================================================

def process_order_webhook(data: dict):
    resource = data.get("resource", "")
    order_id = resource.split("/")[-1]

    if not order_id:
        logger.error("No se pudo extraer order_id del webhook")
        return

    if should_skip_recent_order(order_id):
        logger.info(f"[{order_id}] Webhook duplicado reciente, omitido")
        return

    try:
        logger.info(f"[{order_id}] Procesando orden...")
        order = get_ml_order(order_id)

        if order.get("status") != "paid":
            logger.info(f"[{order_id}] Orden no pagada. Status: {order.get('status')}")
            return

        shipping = order.get("shipping", {}) or {}
        shipment_id = shipping.get("id")

        if not shipment_id:
            logger.info(f"[{order_id}] Sin shipment_id todavía")
            return

        shipment = get_ml_shipment(str(shipment_id))
        shipment_status = shipment.get("status", "")
        shipment_substatus = shipment.get("substatus", "")
        logger.info(f"[{order_id}] Envío: status={shipment_status} substatus={shipment_substatus}")

        if not shipment_es_documentable(shipment):
            logger.info(f"[{order_id}] No documentable aún. status={shipment_status} substatus={shipment_substatus}")
            return

        logger.info(f"[{order_id}] Envío documentable, obteniendo billing...")
        billing = get_ml_billing_info(order_id)
        result = create_document_in_odoo(order, billing)
        logger.info(f"[{order_id}] Resultado: {result}")

    except requests.HTTPError as e:
        detail = e.response.text if e.response is not None else str(e)
        logger.error(f"[{order_id}] HTTPError: {detail}", exc_info=True)
    except Exception as e:
        logger.error(f"[{order_id}] Error inesperado: {e}", exc_info=True)


# =========================================================
# Webhook Mercado Libre
# =========================================================

@app.post("/ml/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    logger.info(f"Webhook recibido: topic={data.get('topic')} resource={data.get('resource')}")

    if data.get("topic") != "orders_v2":
        return {"ok": True, "message": "Topic ignorado"}

    background_tasks.add_task(process_order_webhook, data)
    return {"ok": True, "message": "Recibido"}
