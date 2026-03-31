from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import os
import requests
import xmlrpc.client
from typing import Optional

app = FastAPI()


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


@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/ml/test")
def test_ml():
    try:
        token = get_env("ML_ACCESS_TOKEN")
        seller_id = "70127647"

        r = requests.get(
            f"https://api.mercadolibre.com/orders/search?seller={seller_id}&limit=1",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        return {
            "status_code": r.status_code,
            "response": data,
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Exception", "detail": str(e)},
        )


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")

    try:
        client_id = get_env("ML_CLIENT_ID")
        client_secret = get_env("ML_CLIENT_SECRET")
        redirect_uri = get_env("ML_REDIRECT_URI")
    except RuntimeError as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Configuración incompleta en Railway",
                "detail": str(e),
            },
        )

    token_url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "redirect_uri": redirect_uri,
    }

    try:
        response = requests.post(token_url, data=payload, timeout=30)
        try:
            data = response.json()
        except Exception:
            data = {"raw": response.text}

        return JSONResponse(
            status_code=response.status_code,
            content={
                "status_code": response.status_code,
                "response": data,
            },
        )
    except requests.RequestException as e:
        return JSONResponse(
            status_code=500,
            content={
                "error": "No se pudo conectar con Mercado Libre",
                "detail": str(e),
            },
        )


def ml_headers():
    token = get_env("ML_ACCESS_TOKEN")
    return {"Authorization": f"Bearer {token}"}


def get_ml_order(order_id: str) -> dict:
    response = requests.get(
        f"https://api.mercadolibre.com/orders/{order_id}",
        headers=ml_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_ml_billing_info(order_id: str) -> dict:
    response = requests.get(
        f"https://api.mercadolibre.com/orders/{order_id}/billing_info",
        headers=ml_headers(),
        timeout=30,
    )
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return response.json()


def get_ml_shipment(shipment_id: str) -> dict:
    response = requests.get(
        f"https://api.mercadolibre.com/shipments/{shipment_id}",
        headers=ml_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def extract_rut_from_billing_info(billing: dict) -> str:
    candidates = [
        billing.get("doc_number"),
        billing.get("billing_info", {}).get("doc_number")
        if isinstance(billing.get("billing_info"), dict)
        else None,
        billing.get("additional_info", {}).get("doc_number")
        if isinstance(billing.get("additional_info"), dict)
        else None,
    ]

    for candidate in candidates:
        rut = normalize_rut(candidate or "")
        if rut:
            return rut

    return ""


def odoo_connect():
    odoo_url = get_env("ODOO_URL")
    odoo_db = get_env("ODOO_DB")
    odoo_user = get_env("ODOO_USER")
    odoo_api_key = get_env("ODOO_API_KEY")

    common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
    uid = common.authenticate(odoo_db, odoo_user, odoo_api_key, {})
    if not uid:
        raise Exception("No se pudo autenticar en Odoo")

    models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")
    return odoo_db, odoo_api_key, uid, models


def find_partner_by_rut(models, odoo_db, uid, odoo_api_key, rut: str) -> Optional[int]:
    rut = normalize_rut(rut)
    if not rut:
        return None

    ids = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "search",
        [[["vat", "=", rut]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def find_partner_by_buyer_id(models, odoo_db, uid, odoo_api_key, buyer_id: str) -> Optional[int]:
    if not buyer_id:
        return None

    ids = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "search",
        [[["comment", "ilike", f"ML_BUYER_ID:{buyer_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def append_ml_buyer_id_to_partner(models, odoo_db, uid, odoo_api_key, partner_id: int, buyer_id: str):
    if not buyer_id:
        return

    data = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "read",
        [[partner_id], ["comment"]],
    )

    if not data:
        return

    current_comment = data[0].get("comment") or ""
    marker = f"ML_BUYER_ID:{buyer_id}"

    if marker in current_comment:
        return

    new_comment = f"{current_comment}\n{marker}".strip()

    models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "write",
        [[partner_id], {"comment": new_comment}],
    )


def update_partner_missing_data(models, odoo_db, uid, odoo_api_key, partner_id: int, buyer: dict, rut: str):
    data = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "read",
        [[partner_id], ["name", "vat"]],
    )

    if not data:
        return

    current = data[0]
    vals = {}

    if not current.get("vat") and rut:
        vals["vat"] = rut

    if not current.get("name") or current.get("name") == "Cliente Mercado Libre":
        suggested_name = buyer.get("nickname") or buyer.get("first_name") or "Cliente Mercado Libre"
        vals["name"] = suggested_name

    if vals:
        models.execute_kw(
            odoo_db,
            uid,
            odoo_api_key,
            "res.partner",
            "write",
            [[partner_id], vals],
        )


def create_partner(models, odoo_db, uid, odoo_api_key, buyer: dict, rut: str) -> int:
    buyer_id = str(buyer.get("id", ""))
    partner_name = buyer.get("nickname") or buyer.get("first_name") or "Cliente Mercado Libre"

    vals = {
        "name": partner_name,
        "vat": rut or False,
        "comment": f"ML_BUYER_ID:{buyer_id}" if buyer_id else False,
        "customer_rank": 1,
    }

    return models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "res.partner",
        "create",
        [vals],
    )


def find_or_create_partner(buyer: dict, billing: dict) -> int:
    odoo_db, odoo_api_key, uid, models = odoo_connect()

    buyer_id = str(buyer.get("id", ""))
    rut = extract_rut_from_billing_info(billing)

    partner_id = find_partner_by_rut(models, odoo_db, uid, odoo_api_key, rut)
    if partner_id:
        append_ml_buyer_id_to_partner(models, odoo_db, uid, odoo_api_key, partner_id, buyer_id)
        update_partner_missing_data(models, odoo_db, uid, odoo_api_key, partner_id, buyer, rut)
        return partner_id

    partner_id = find_partner_by_buyer_id(models, odoo_db, uid, odoo_api_key, buyer_id)
    if partner_id:
        update_partner_missing_data(models, odoo_db, uid, odoo_api_key, partner_id, buyer, rut)
        return partner_id

    return create_partner(models, odoo_db, uid, odoo_api_key, buyer, rut)


def find_existing_invoice(order_id: str) -> Optional[int]:
    odoo_db, odoo_api_key, uid, models = odoo_connect()

    ids = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "account.move",
        "search",
        [[["ref", "=", f"ML-{order_id}"]]],
        {"limit": 1},
    )
    return ids[0] if ids else None


def create_invoice_in_odoo(order: dict, billing: dict) -> dict:
    order_id = str(order["id"])

    existing = find_existing_invoice(order_id)
    if existing:
        return {"ok": True, "message": "Factura ya existe", "invoice_id": existing}

    odoo_db, odoo_api_key, uid, models = odoo_connect()

    buyer = order.get("buyer", {})
    partner_id = find_or_create_partner(buyer, billing)

    lines = []
    for row in order.get("order_items", []):
        title = row.get("item", {}).get("title", "Producto Mercado Libre")
        qty = row.get("quantity", 1)
        unit_price_gross = float(row.get("unit_price", 0))
        unit_price_net = round(unit_price_gross / 1.19, 2)

        lines.append(
            (
                0,
                0,
                {
                    "name": title,
                    "quantity": qty,
                    "price_unit": unit_price_net,
                },
            )
        )

    if not lines:
        raise Exception("La orden no tiene líneas para facturar")

    invoice_vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "ref": f"ML-{order_id}",
        "invoice_line_ids": lines,
    }

    invoice_id = models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "account.move",
        "create",
        [invoice_vals],
    )

    models.execute_kw(
        odoo_db,
        uid,
        odoo_api_key,
        "account.move",
        "action_post",
        [[invoice_id]],
    )

    return {"ok": True, "message": "Factura creada", "invoice_id": invoice_id}


@app.post("/ml/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Body inválido"})

    topic = data.get("topic")
    resource = data.get("resource", "")

    if topic != "orders_v2":
        return {"ok": True, "message": "Topic ignorado"}

    order_id = resource.split("/")[-1]
    if not order_id:
        raise HTTPException(status_code=400, detail="No se pudo extraer order_id")

    try:
        order = get_ml_order(order_id)

        if order.get("status") != "paid":
            return {
                "ok": True,
                "message": f"Orden no facturable todavía. Status orden: {order.get('status')}",
            }

        shipping = order.get("shipping", {}) or {}
        shipment_id = shipping.get("id")

        if not shipment_id:
            return {"ok": True, "message": "La orden no tiene shipment_id todavía"}

        shipment = get_ml_shipment(str(shipment_id))
        shipment_status = shipment.get("status")

        if shipment_status != "shipped":
            return {
                "ok": True,
                "message": f"No se factura aún. Status envío: {shipment_status}",
            }

        billing = get_ml_billing_info(order_id)
        result = create_invoice_in_odoo(order, billing)
        return result

    except requests.HTTPError as e:
        detail = e.response.text if e.response is not None else str(e)
        return JSONResponse(
            status_code=500,
            content={"error": "HTTPError", "detail": detail},
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": "Exception", "detail": str(e)},
        )
