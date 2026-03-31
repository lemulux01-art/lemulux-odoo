from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import os
import requests

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


@app.get("/")
def root():
    return {"status": "ok", "service": "lemulux-odoo"}


@app.get("/health")
def health():
    return {"status": "healthy"}


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
                "sent_payload": {
                    "grant_type": payload["grant_type"],
                    "client_id": payload["client_id"],
                    "code_present": True,
                    "redirect_uri": payload["redirect_uri"],
                },
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


@app.post("/ml/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {"raw": "no-json"}

    return {
        "ok": True,
        "received": data,
    }
