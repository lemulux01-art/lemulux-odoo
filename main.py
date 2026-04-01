from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import os
import logging
import requests
import xmlrpc.client
from typing import Optional, Any
from dataclasses import dataclass

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


def recursive_find_matching_dict(data: Any, key: str, contains_text: str) -> Optional[dict]:
    contains_text = contains_text.lower()

    if isinstance(data, dict):
        value = data.get(key)
        if isinstance(value, str) and contains_text in value.lower():
            return data
        for _, child in data.items():
            found = recursive_find_matching_dict(child, key, contains_text)
            if found:
                return found

    elif isinstance(data, list):
        for child in data:
            found = recursive_find_matching_dict(child, key, contains_text)
            if found:
                return found

    return None


# =========================================================
# Validación al arrancar
# =========================================================

@app.on_event("startup")
async def validate_env():
    missing = [k for k in REQUIRED_ENV_VARS if not os.getenv(k, "").strip()]
    if missing:
        raise RuntimeError(f"❌ Variables de entorno faltantes al iniciar: {missing}")
    logger.info("✅ Todas las variables de entorno cargadas correctamente")


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
# Persistencia opcional de tokens en Railway
# =========================================================

def persist_tokens_to_railway(access_token: str, refresh_token: str):
    railway_api_token = os.getenv("RAILWAY_API_TOKEN", "").strip()
    project_id = os.getenv("RAILWAY_PROJECT_ID", "").strip()
    environment_id = os.getenv("RAILWAY_ENVIRONMENT_ID", "").strip()
    service_id = os.getenv("RAILWAY_SERVICE_ID", "").strip()

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
            headers={
                "Authorization": f"Bearer {railway_api_token}",
                "Content-Type": "application/json",
            },
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("errors"):
            logger.error(f"❌ Railway API error al persistir tokens: {data['errors']}")
        else:
            logger.info("✅ Tokens ML persistidos en Railway correctamente")
    except Exception as e:
        logger.error(f"❌ Error al persistir tokens en Railway: {e}", exc_info=True)


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
            logger.error("❌ Respuesta sin access_token al renovar token")
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


def ml_get(url: str) -> dict:
    response = requests.get(url, headers=ml_headers(), timeout=30)
    if response.status_code == 401:
        logger.warning(f"Token ML expirado al consultar {url}, renovando...")
        if not refresh_ml_token():
            raise Exception("No se pudo renovar el token de Mercado Libre")
        response = requests.get(url, headers=ml_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_billing_info(order_id: str) -> dict:
    url = f"https://api.mercadolibre.com/orders/{order_id}/billing_info"
    response = requests.get(url, headers=ml_headers(), timeout=30)
    if response.status_code == 401:
        logger.warning("Token ML expirado al obtener billing_info, renovando...")
        if not refresh_ml_token():
            raise Exception("No se pudo renovar el token de Mercado Libre")
        response = requests.get(url, headers=ml_headers(), timeout=30)
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json()


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
            if access_token and refresh_token:
                persist_tokens_to_railway(access_token, refresh_token)
            logger.info("✅ Tokens OAuth guardados en memoria y Railway")

        return JSONResponse(
            status_code=response.status_code,
            content={"status_code": response.status_code, "response": data},
        )

    except requests.RequestException as e:
        return JSONResponse(
            status_code=500,
            content={"error": "No se pudo conectar con Mercado Libre", "detail": str(e)},
        )


@app.post("/ml/refresh-token")
def manual_refresh_token():
    success = refresh_ml_token()
    if success:
        return {"ok": True, "message": "Token renovado y persistido en Railway"}
    return JSONResponse(
        status_code=500,
        content={"ok": False, "message": "No se pudo renovar el token"},
    )


# =========================================================
# Extracción de datos desde billing_info
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
    return first_non_empty(
        recursive_find_first(billing, ["business_activity"]),
        recursive_find_first(billing, ["activity"]),
        recursive_find_first(billing, ["economic_activity"]),
        recursive_find_first(billing, ["taxpayer_activity"]),
        recursive_find_first(billing, ["activity_description"]),
        recursive_find_first(billing, ["giro"]),
        recursive_find_first(billing, ["description_of_activity"]),
    )


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
    taxpayer_type = extract_taxpayer_type_from_billing_info(billing)
    markers = [
        "consumidor final",
        "final consumer",
        "consumer final",
        "cf",
    ]
    return any(marker in taxpayer_type for marker in markers)


def ml_looks_like_company_name(name: str) -> bool:
    name = (name or "").upper()
    company_markers = [
        " SPA", " EIRL", " LTDA", " S.A", " S.A.", " SOCIEDAD",
        " COMERCIAL", " COMERCIALIZADORA", " CONSTRUCTORA",
        " TRANSPORTES", " INVERSIONES", " IMPORTADORA", " EXPORTADORA",
    ]
    return any(marker in f" {name}" for marker in company_markers)


def should_be_company_from_ml(billing: dict, buyer: dict) -> bool:
    if ml_indicates_final_consumer(billing):
        return False

    activity = extract_activity_from_billing_info(billing)
    if activity:
        return True

    name = extract_name_from_billing_info(billing, buyer)
    if ml_looks_like_company_name(name):
        return True

    return False


# =========================================================
# Odoo contexto
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
        ctx.db,
        ctx.uid,
        ctx.api_key,
        model,
        method,
        args,
        kwargs or {},
    )


def model_has_field(ctx: OdooCtx, model: str, field_name: str) -> bool:
    fields_info = odoo_exec(ctx, model, "fields_get", [[]], {"attributes": ["type"]})
    return field_name in fields_info


def get_selection_values(ctx: OdooCtx, model: str, field_name: str) -> list:
    fields_info = odoo_exec(ctx, model, "fields_get", [[]], {"attributes": ["selection"]})
    field = fields_info.get(field_name, {})
    return field.get("selection", []) or []


# =========================================================
# Odoo helpers fiscales Chile
# =========================================================

def get_chile_country_id(ctx: OdooCtx) -> int:
    ids = odoo_exec(
        ctx,
        "res.country",
        "search",
        [[["code", "=", "CL"]]],
        {"limit": 1},
    )
    if not ids:
        raise Exception("No se encontró el país Chile en Odoo")
    return ids[0]


def get_rut_identification_type_id(ctx: OdooCtx) -> Optional[int]:
    # Intenta encontrar el tipo de identificación RUT
    candidates = [
        [["name", "ilike", "RUT"]],
        [["display_name", "ilike", "RUT"]],
    ]
    for domain in candidates:
        ids = odoo_exec(
            ctx,
            "l10n_latam.identification.type",
            "search",
            [[domain]],
            {"limit": 1},
        )
        if ids:
            return ids[0]
    return None


def get_or_create_region_state_id(ctx: OdooCtx, state_name: str, country_id: int) -> Optional[int]:
    state_name = (state_name or "").strip()
    if not state_name:
        return None

    ids = odoo_exec(
        ctx,
        "res.country.state",
        "search",
        [[["name", "ilike", state_name], ["country_id", "=", country_id]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def extract_address_from_order(order: dict) -> dict:
    shipping = order.get("shipping", {}) or {}
    receiver = shipping.get("receiver_address", {}) or {}

    street_name = receiver.get("street_name") or ""
    street_number = receiver.get("street_number") or ""
    address_line = first_non_empty(
        f"{street_name} {street_number}".strip(),
        receiver.get("address_line"),
    )

    city = first_non_empty(
        receiver.get("city", {}).get("name") if isinstance(receiver.get("city"), dict) else "",
        receiver.get("city"),
    )
    state = first_non_empty(
        receiver.get("state", {}).get("name") if isinstance(receiver.get("state"), dict) else "",
        receiver.get("state"),
    )
    zip_code = receiver.get("zip_code") or ""
    comment = receiver.get("comment") or ""

    return {
        "street": address_line,
        "city": city,
        "state_name": state,
        "zip": zip_code,
        "comment": comment,
    }


def get_consumer_final_value_for_partner(ctx: OdooCtx) -> Optional[str]:
    """
    Intenta detectar el valor correcto del campo tributario chileno si existe.
    """
    candidate_fields = [
        "l10n_cl_sii_taxpayer_type",
        "l10n_cl_taxpayer_type",
    ]

    for field_name in candidate_fields:
        if not model_has_field(ctx, "res.partner", field_name):
            continue

        options = get_selection_values(ctx, "res.partner", field_name)
        for value, label in options:
            label_l = str(label).lower()
            value_l = str(value).lower()
            if "consumidor final" in label_l or "final consumer" in label_l:
                return (field_name, value)
            if value_l in {"cf", "consumer", "final_consumer", "consumidor_final"}:
                return (field_name, value)

    return None


# =========================================================
# Partners / Clientes
# =========================================================

def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    rut = normalize_rut(rut)
    if not rut:
        return None

    ids = odoo_exec(
        ctx,
        "res.partner",
        "search",
        [[["vat", "=", rut]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def find_partner_by_buyer_id(ctx: OdooCtx, buyer_id: str) -> Optional[int]:
    if not buyer_id:
        return None

    ids = odoo_exec(
        ctx,
        "res.partner",
        "search",
        [[["comment", "ilike", f"ML_BUYER_ID:{buyer_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def read_partner(ctx: OdooCtx, partner_id: int) -> dict:
    fields = ["name", "vat", "email", "comment", "company_type", "is_company", "country_id"]
    if model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        fields.append("l10n_latam_identification_type_id")
    if model_has_field(ctx, "res.partner", "state_id"):
        fields.append("state_id")

    data = odoo_exec(
        ctx,
        "res.partner",
        "read",
        [[partner_id], fields],
    )
    return data[0] if data else {}


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


def build_partner_vals_from_ml(ctx: OdooCtx, buyer: dict, billing: dict, order: dict, rut: str) -> dict:
    buyer_id = str(buyer.get("id", ""))
    partner_name = extract_name_from_billing_info(billing, buyer)
    is_company = should_be_company_from_ml(billing, buyer)
    chile_country_id = get_chile_country_id(ctx)
    rut_identification_type_id = get_rut_identification_type_id(ctx)
    address = extract_address_from_order(order)
    state_id = get_or_create_region_state_id(ctx, address.get("state_name", ""), chile_country_id)

    vals = {
        "name": partner_name,
        "vat": rut or False,
        "email": ML_DEFAULT_EMAIL,
        "comment": f"ML_BUYER_ID:{buyer_id}" if buyer_id else False,
        "customer_rank": 1,
        "company_type": "company" if is_company else "person",
        "is_company": True if is_company else False,
        "country_id": chile_country_id,
    }

    if address.get("street"):
        vals["street"] = address["street"]
    if address.get("city"):
        vals["city"] = address["city"]
    if address.get("zip"):
        vals["zip"] = address["zip"]
    if state_id and model_has_field(ctx, "res.partner", "state_id"):
        vals["state_id"] = state_id

    if rut_identification_type_id and model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        vals["l10n_latam_identification_type_id"] = rut_identification_type_id

    # Si es consumidor final, intenta setear el campo fiscal chileno correspondiente
    if ml_indicates_final_consumer(billing):
        cf_field_value = get_consumer_final_value_for_partner(ctx)
        if cf_field_value:
            field_name, field_value = cf_field_value
            vals[field_name] = field_value

    return vals


def update_partner_missing_data(ctx: OdooCtx, partner_id: int, buyer: dict, billing: dict, order: dict, rut: str):
    current = read_partner(ctx, partner_id)
    desired = build_partner_vals_from_ml(ctx, buyer, billing, order, rut)
    vals = {}

    for key, value in desired.items():
        current_value = current.get(key)

        # Many2one leído viene como [id, name]
        if isinstance(current_value, list) and current_value:
            current_value = current_value[0]

        if key in {"comment"}:
            # comment se maneja aparte
            continue

        if key not in current or current_value in (False, None, "", []) or current_value != value:
            vals[key] = value

    if vals:
        odoo_exec(ctx, "res.partner", "write", [[partner_id], vals])


def create_partner(ctx: OdooCtx, buyer: dict, billing: dict, order: dict, rut: str) -> int:
    vals = build_partner_vals_from_ml(ctx, buyer, billing, order, rut)
    return odoo_exec(ctx, "res.partner", "create", [vals])


def find_or_create_partner(ctx: OdooCtx, buyer: dict, billing: dict, order: dict) -> tuple[int, dict]:
    buyer_id = str(buyer.get("id", ""))
    rut = extract_rut_from_billing_info(billing)

    partner_id = find_partner_by_rut(ctx, rut)
    if partner_id:
        logger.info(f"Partner encontrado por RUT {rut}: partner_id={partner_id}")
        append_ml_buyer_id_to_partner(ctx, partner_id, buyer_id)
        update_partner_missing_data(ctx, partner_id, buyer, billing, order, rut)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = find_partner_by_buyer_id(ctx, buyer_id)
    if partner_id:
        logger.info(f"Partner encontrado por ML_BUYER_ID {buyer_id}: partner_id={partner_id}")
        update_partner_missing_data(ctx, partner_id, buyer, billing, order, rut)
        return partner_id, read_partner(ctx, partner_id)

    partner_id = create_partner(ctx, buyer, billing, order, rut)
    logger.info(f"Partner creado para RUT {rut}: partner_id={partner_id}")
    return partner_id, read_partner(ctx, partner_id)


# =========================================================
# Decisión factura / boleta
# =========================================================

def partner_is_company(partner_data: dict) -> bool:
    if not partner_data:
        return False
    return partner_data.get("company_type") == "company" or partner_data.get("is_company") is True


def decide_document_kind(partner_data: dict, billing: dict) -> tuple[str, str]:
    rut = normalize_rut(partner_data.get("vat") or "")
    activity = extract_activity_from_billing_info(billing)

    if ml_indicates_final_consumer(billing):
        return "boleta", "ML indica consumidor final"

    if partner_data.get("company_type") == "person":
        return "boleta", "Cliente clasificado como persona"

    if partner_is_company(partner_data) and rut and activity:
        return "factura", "Empresa con RUT y actividad económica"

    return "boleta", "Fallback seguro a boleta"


# =========================================================
# Documentos
# =========================================================

def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "account.move",
        "search",
        [[
            ["ref", "=", f"ML-{order_id}"],
            ["state", "in", ["posted", "cancel"]],
        ]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_document_type_id(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_DOC_TYPE_FACTURA_ID", required=True)
    if document_kind == "boleta":
        return to_int_env("ODOO_DOC_TYPE_BOLETA_ID", required=True)
    raise Exception(f"Tipo de documento no soportado: {document_kind}")


def create_account_move(ctx: OdooCtx, order: dict, partner_id: int, document_kind: str) -> int:
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
    order_id = str(order["id"])
    ctx = odoo_connect()

    existing = find_existing_move(ctx, order_id)
    if existing:
        logger.info(f"[{order_id}] Documento ya existe: move_id={existing}")
        return {"ok": True, "message": "Documento ya existe", "move_id": existing}

    buyer = order.get("buyer", {}) or {}
    partner_id, partner_data = find_or_create_partner(ctx, buyer, billing, order)

    document_kind, reason = decide_document_kind(partner_data, billing)
    logger.info(f"[{order_id}] Documento decidido: {document_kind} — {reason}")

    move_id = create_account_move(ctx, order, partner_id, document_kind)
    logger.info(f"[{order_id}] Documento creado en Odoo: move_id={move_id}")

    return {
        "ok": True,
        "message": f"{document_kind.capitalize()} creada",
        "document_kind": document_kind,
        "reason": reason,
        "move_id": move_id,
    }


# =========================================================
# Procesamiento desde venta registrada
# =========================================================

def process_order_webhook(data: dict):
    resource = data.get("resource", "")
    order_id = resource.split("/")[-1]

    if not order_id:
        logger.error("No se pudo extraer order_id del webhook")
        return

    try:
        logger.info(f"[{order_id}] Procesando orden desde venta registrada...")
        order = get_ml_order(order_id)

        if order.get("status") != "paid":
            logger.info(f"[{order_id}] Orden no pagada. Status: {order.get('status')}")
            return

        logger.info(f"[{order_id}] Venta registrada/pagada. Creando documento...")
        billing = get_ml_billing_info(order_id)
        result = create_document_in_odoo(order, billing)
        logger.info(f"[{order_id}] Resultado final: {result}")

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

    topic = data.get("topic")
    if topic != "orders_v2":
        return {"ok": True, "message": "Topic ignorado"}

    background_tasks.add_task(process_order_webhook, data)
    return {"ok": True, "message": "Recibido"}
