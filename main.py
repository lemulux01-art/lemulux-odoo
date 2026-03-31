@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")

    print("CLIENT_ID:", ML_CLIENT_ID)
    print("CLIENT_SECRET:", ML_CLIENT_SECRET)

    if not code:
        raise HTTPException(status_code=400, detail="No se recibió code")

    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    print("PAYLOAD:", payload)

    response = requests.post("https://api.mercadolibre.com/oauth/token", data=payload)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }
