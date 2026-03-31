from fastapi import FastAPI, Request, HTTPException
import os
import requests
import xmlrpc.client

app = FastAPI()

ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI")

ODOO_URL = os.getenv("ODOO_URL")
ODOO_DB = os.getenv("ODOO_DB")
ODOO_USER = os.getenv("ODOO_USER")
ODOO_API_KEY = os.getenv("ODOO_API_KEY")

ML_ACCESS_TOKEN = os.getenv("ML_ACCESS_TOKEN")


def odoo_connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
    if not uid:
        raise Exception("No se pudo autenticar en Odoo")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")

    token_url = "https://api.mercadolibre.com/oauth/token"
    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    response = requests.post(token_url, data=payload, timeout=30)

    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}

    return {
        "status_code": response.status_code,
        "response": data
    }


def ml_headers():
    return {"Authorization": f"Bearer {ML_ACCESS_TOKEN}"}


def get_ml_order(order_id: str):
    response = requests.get(
        f"https://api.mercadolibre.com/orders/{order_id}",
        headers=ml_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def get_ml_shipment(shipment_id: str):
    response = requests.get(
        f"https://api.mercadolibre.com/shipments/{shipment_id}",
        headers=ml_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def find_existing_invoice(uid, models, order_id: str):
    ids = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "account.move", "search",
        [[["ref", "=", f"ML-{order_id}"]]],
        {"limit": 1}
    )
    return ids[0] if ids else None


def find_or_create_partner(uid, models, buyer):
    buyer_id = str(buyer.get("id", ""))
    partner_name = (
        buyer.get("nickname")
        or buyer.get("first_name")
        or "Cliente Mercado Libre"
    )

    partner_ids = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "res.partner", "search",
        [[["comment", "=", f"ML_BUYER_ID:{buyer_id}"]]],
        {"limit": 1}
    )
    if partner_ids:
        return partner_ids[0]

    vals = {
        "name": partner_name,
        "comment": f"ML_BUYER_ID:{buyer_id}",
        "customer_rank": 1,
    }

    return models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "res.partner", "create",
        [vals]
    )


def create_invoice_in_odoo(order):
    uid, models = odoo_connect()
    order_id = str(order["id"])

    existing_invoice_id = find_existing_invoice(uid, models, order_id)
    if existing_invoice_id:
        return {
            "ok": True,
            "message": "Factura ya existe",
            "invoice_id": existing_invoice_id
        }

    buyer = order.get("buyer", {})
    partner_id = find_or_create_partner(uid, models, buyer)

    lines = []
    for row in order.get("order_items", []):
        title = row.get("item", {}).get("title", "Producto Mercado Libre")
        qty = row.get("quantity", 1)
        unit_price_gross = float(row.get("unit_price", 0))
        unit_price_net = round(unit_price_gross / 1.19, 2)

        lines.append((0, 0, {
            "name": title,
            "quantity": qty,
            "price_unit": unit_price_net,
        }))

    if not lines:
        raise Exception("La orden no tiene líneas para facturar")

    invoice_vals = {
        "move_type": "out_invoice",
        "partner_id": partner_id,
        "ref": f"ML-{order_id}",
        "invoice_line_ids": lines,
    }

    invoice_id = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "account.move", "create",
        [invoice_vals]
    )

    models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "account.move", "action_post",
        [[invoice_id]]
    )

    return {
        "ok": True,
        "message": "Factura creada",
        "invoice_id": invoice_id
    }


@app.post("/ml/webhook")
async def webhook(request: Request):
    data = await request.json()

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
                "message": f"Orden no facturable aún. Status orden: {order.get('status')}"
            }

        shipping = order.get("shipping", {}) or {}
        shipment_id = shipping.get("id")

        if not shipment_id:
            return {
                "ok": True,
                "message": "La orden no tiene shipment_id aún"
            }

        shipment = get_ml_shipment(str(shipment_id))
        shipment_status = shipment.get("status")

        if shipment_status != "shipped":
            return {
                "ok": True,
                "message": f"Aún no se factura. Status envío: {shipment_status}"
            }

        result = create_invoice_in_odoo(order)
        return result

    except requests.HTTPError as e:
        detail = e.response.text if e.response is not None else str(e)
        raise HTTPException(status_code=500, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
