@app.get("/ml/test")
def test_ml():
    try:
        token = get_env("ML_ACCESS_TOKEN")

        # user_id de tu autorización exitosa
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
