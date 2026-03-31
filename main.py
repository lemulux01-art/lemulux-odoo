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

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


def get_ml_order(order_id):
    response = requests.get(
        f"https://api.mercadolibre.com/orders/{order_id}",
        headers={"Authorization": f"Bearer {ML_ACCESS_TOKEN}"},
    )
    return response.json()


def create_invoice(order):
    uid, models = odoo_connect()

    partner_id = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "res.partner", "create",
        [{
            "name": "Cliente MercadoLibre",
            "customer_rank": 1
        }]
    )

    lines = []
    for item in order["order_items"]:
        lines.append((0, 0, {
            "name": item["item"]["title"],
            "quantity": item["quantity"],
            "price_unit": round(item["unit_price"] / 1.19, 2)
        }))

    invoice_id = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "account.move", "create",
        [{
            "move_type": "out_invoice",
            "partner_id": partner_id,
            "invoice_line_ids": lines,
        }]
    )

    models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "account.move", "action_post",
        [[invoice_id]]
    )

    return invoice_id


@app.post("/ml/webhook")
async def webhook(request: Request):
    data = await request.json()

    if data.get("topic") != "orders_v2":
        return {"ok": True}

    order_id = data["resource"].split("/")[-1]

    order = get_ml_order(order_id)

    if order.get("status") != "paid":
        return {"ok": True, "message": "Aún no pagado"}

    invoice_id = create_invoice(order)

    return {"ok": True, "invoice_id": invoice_id}
