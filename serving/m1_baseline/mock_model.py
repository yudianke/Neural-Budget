def predict_category(payload):
    merchant = payload.merchant.lower()
    hint = payload.historical_majority_category_for_payee.lower()

    if "subway" in merchant or "pizza" in merchant or "burger" in merchant:
        pred = "restaurants"
        conf = 0.91
        top3 = [
            {"category": "restaurants", "confidence": 0.91},
            {"category": "groceries", "confidence": 0.05},
            {"category": "other", "confidence": 0.04},
        ]
    elif "tesco" in merchant or "walmart" in merchant or "target" in merchant:
        pred = "groceries"
        conf = 0.88
        top3 = [
            {"category": "groceries", "confidence": 0.88},
            {"category": "household", "confidence": 0.08},
            {"category": "other", "confidence": 0.04},
        ]
    elif hint:
        pred = hint
        conf = 0.75
        top3 = [
            {"category": hint, "confidence": 0.75},
            {"category": "other", "confidence": 0.15},
            {"category": "groceries", "confidence": 0.10},
        ]
    else:
        pred = "other"
        conf = 0.55
        top3 = [
            {"category": "other", "confidence": 0.55},
            {"category": "groceries", "confidence": 0.25},
            {"category": "restaurants", "confidence": 0.20},
        ]

    return {
        "transaction_id": payload.transaction_id,
        "synthetic_user_id": payload.synthetic_user_id,
        "predicted_category": pred,
        "confidence": conf,
        "top_3_suggestions": top3,
    }
