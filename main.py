from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    return {"message": "ok", "code": code}

@app.post("/ml/webhook")
async def webhook(request: Request):
    data = await request.json()
    print("ML Webhook:", data)
    return {"ok": True}
