from fastapi import FastAPI, Request, HTTPException
import requests
import os

app = FastAPI()

ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")
ML_CLIENT_SECRET = os.getenv("ML_CLIENT_SECRET")
ML_REDIRECT_URI = os.getenv("ML_REDIRECT_URI")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")

    if not ML_CLIENT_ID or not ML_CLIENT_SECRET or not ML_REDIRECT_URI:
        return {
            "error": "Faltan variables en Railway",
            "ML_CLIENT_ID": ML_CLIENT_ID,
            "ML_CLIENT_SECRET_present": bool(ML_CLIENT_SECRET),
            "ML_REDIRECT_URI": ML_REDIRECT_URI,
        }

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
        "sent_payload": {
            "grant_type": payload["grant_type"],
            "client_id": payload["client_id"],
            "code_present": bool(payload["code"]),
            "redirect_uri": payload["redirect_uri"],
        },
        "status_code": response.status_code,
        "response": data
    }
