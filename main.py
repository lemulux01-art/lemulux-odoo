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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("lemulux")

app = FastAPI()

IVA_RATE = 1.19
ML_DEFAULT_EMAIL = "odoo@lemulux.com"

# cache simple en memoria para evitar reprocesar misma orden muchas veces
RECENT_ORDERS = {}
RECENT_ORDERS_LOCK = Lock()
DEDUP_SECONDS = 60


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


@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


def ml_headers() -> dict:
    return {"Authorization": f"Bearer {get_env('ML_ACCESS_TOKEN')}"}


def ml_get(url: str, retries: int = 4, backoff: int = 2) -> dict:
    for attempt in range(retries):
        response = requests.get(url, headers=ml_headers(), timeout=30)

        if response.status_code == 429:
            wait_time = backoff * (attempt + 1)
            logger.warning(f"429 Too Many Requests en {url}. Reintento en {wait_time}s")
            time.sleep(wait_time)
            continue

        if response.status_code == 401:
            raise Exception("Token ML inválido o expirado")

        response.raise_for_status()
        return response.json()

    raise Exception(f"Mercado Libre devolvió 429 demasiadas veces para {url}")


def get_ml_order(order_id: str) -> dict:
    return ml_get(f"https://api.mercadolibre.com/orders/{order_id}")


def get_ml_billing_info(order_id: str) -> dict:
    url = f"https://api.mercadolibre.com/orders/{order_id}/billing_info"

    for attempt in range(4):
        response = requests.get(url, headers=ml_headers(), timeout=30)

        if response.status_code == 404:
            return {}

        if response.status_code == 429:
            wait_time = 2 * (attempt + 1)
            logger.warning(f"429 en billing_info {order_id}. Reintento en {wait_time}s")
            time.sleep(wait_time)
            continue

        if response.status_code == 401:
            raise Exception("Token ML inválido o expirado")

        response.raise_for_status()
        return response.json()

    raise Exception(f"Mercado Libre devolvió 429 demasiadas veces en billing_info para {order_id}")


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


def get_chile_country_id(ctx: OdooCtx) -> int:
    ids = odoo_exec(
        ctx,
        "res.country",
        "search",
        [[["code", "=", "CL"]]],
        {"limit": 1},
    )
    if not ids:
        raise Exception("No se encontró Chile en Odoo")
    return ids[0]


def get_rut_identification_type_id(ctx: OdooCtx) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "l10n_latam.identification.type",
        "search",
        [[["name", "ilike", "RUT"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


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
    return any(x in taxpayer_type for x in ["consumidor final", "final consumer", "consumer final", "cf"])


def should_be_company_from_ml(billing: dict, buyer: dict) -> bool:
    if ml_indicates_final_consumer(billing):
        return False
    if extract_activity_from_billing_info(billing):
        return True

    name = extract_name_from_billing_info(billing, buyer).upper()
    return any(x in name for x in ["SPA", "EIRL", "LTDA", "S.A", "SOCIEDAD", "COMERCIAL"])


def find_partner_by_rut(ctx: OdooCtx, rut: str) -> Optional[int]:
    if not rut:
        return None
    ids = odoo_exec(ctx, "res.partner", "search", [[["vat", "=", rut]]], {"limit": 1})
    return ids[0] if ids else None


def read_partner(ctx: OdooCtx, partner_id: int) -> dict:
    fields = ["name", "vat", "comment", "company_type", "is_company", "country_id"]
    if model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        fields.append("l10n_latam_identification_type_id")
    data = odoo_exec(ctx, "res.partner", "read", [[partner_id], fields])
    return data[0] if data else {}


def create_partner(ctx: OdooCtx, buyer: dict, billing: dict, order: dict, rut: str) -> int:
    chile_id = get_chile_country_id(ctx)
    rut_type_id = get_rut_identification_type_id(ctx)

    vals = {
        "name": extract_name_from_billing_info(billing, buyer),
        "vat": rut or False,
        "email": ML_DEFAULT_EMAIL,
        "customer_rank": 1,
        "company_type": "company" if should_be_company_from_ml(billing, buyer) else "person",
        "is_company": True if should_be_company_from_ml(billing, buyer) else False,
        "country_id": chile_id,
    }

    if rut_type_id and model_has_field(ctx, "res.partner", "l10n_latam_identification_type_id"):
        vals["l10n_latam_identification_type_id"] = rut_type_id

    return odoo_exec(ctx, "res.partner", "create", [vals])


def find_or_create_partner(ctx: OdooCtx, buyer: dict, billing: dict, order: dict) -> tuple[int, dict]:
    rut = extract_rut_from_billing_info(billing)
    partner_id = find_partner_by_rut(ctx, rut)

    if partner_id:
        logger.info(f"Partner encontrado por RUT {rut}: partner_id={partner_id}")
        return partner_id, read_partner(ctx, partner_id)

    partner_id = create_partner(ctx, buyer, billing, order, rut)
    logger.info(f"Partner creado para RUT {rut}: partner_id={partner_id}")
    return partner_id, read_partner(ctx, partner_id)


def decide_document_kind(partner_data: dict, billing: dict) -> tuple[str, str]:
    if ml_indicates_final_consumer(billing):
        return "boleta", "ML indica consumidor final"

    rut = normalize_rut(partner_data.get("vat") or "")
    activity = extract_activity_from_billing_info(billing)

    if partner_data.get("company_type") == "company" and rut and activity:
        return "factura", "Empresa con RUT y actividad económica"

    return "boleta", "Fallback seguro a boleta"


def find_existing_move(ctx: OdooCtx, order_id: str) -> Optional[int]:
    ids = odoo_exec(
        ctx,
        "account.move",
        "search",
        [[["ref", "=", f"ML-{order_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def get_document_type_id(document_kind: str) -> int:
    if document_kind == "factura":
        return to_int_env("ODOO_DOC_TYPE_FACTURA_ID")
    return to_int_env("ODOO_DOC_TYPE_BOLETA_ID")


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
        return {"ok": True, "message": "Documento ya existe", "move_id": existing}

    buyer = order.get("buyer", {}) or {}
    partner_id, partner_data = find_or_create_partner(ctx, buyer, billing, order)

    document_kind, reason = decide_document_kind(partner_data, billing)
    logger.info(f"[{order_id}] Documento decidido: {document_kind} — {reason}")

    move_id = create_account_move(ctx, order, partner_id, document_kind)
    logger.info(f"[{order_id}] Documento creado en Odoo: move_id={move_id}")

    return {"ok": True, "move_id": move_id, "document_kind": document_kind}


def should_skip_recent_order(order_id: str) -> bool:
    now = time.time()
    with RECENT_ORDERS_LOCK:
        last = RECENT_ORDERS.get(order_id)
        if last and now - last < DEDUP_SECONDS:
            return True
        RECENT_ORDERS[order_id] = now
        return False


def process_order_webhook(data: dict):
    resource = data.get("resource", "")
    order_id = resource.split("/")[-1]

    if not order_id:
        logger.error("No se pudo extraer order_id del webhook")
        return

    if should_skip_recent_order(order_id):
        logger.info(f"[{order_id}] Webhook duplicado reciente, se omite")
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


@app.post("/ml/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    if data.get("topic") != "orders_v2":
        return {"ok": True, "message": "Topic ignorado"}

    background_tasks.add_task(process_order_webhook, data)
    return {"ok": True, "message": "Recibido"}
