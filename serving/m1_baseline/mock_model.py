from schemas import M1Input, M1Output, CategorySuggestion


def predict_category(x: M1Input) -> M1Output:
    merchant_upper = x.merchant.upper().strip()

    # simple merchant-based rules
    if any(k in merchant_upper for k in ["SUBWAY", "MCDONALD", "BURGER", "KFC", "PIZZA"]):
        pred = "restaurants"
        conf = 0.91
        top3 = [
            CategorySuggestion(category="restaurants", confidence=0.91),
            CategorySuggestion(category="groceries", confidence=0.05),
            CategorySuggestion(category="other", confidence=0.04),
        ]

    elif any(k in merchant_upper for k in ["TESCO", "WALMART", "TARGET", "COSTCO", "TRADER JOE"]):
        pred = "groceries"
        conf = 0.88
        top3 = [
            CategorySuggestion(category="groceries", confidence=0.88),
            CategorySuggestion(category="household", confidence=0.07),
            CategorySuggestion(category="other", confidence=0.05),
        ]

    elif any(k in merchant_upper for k in ["SHELL", "EXXON", "BP ", "CHEVRON"]):
        pred = "transportation"
        conf = 0.86
        top3 = [
            CategorySuggestion(category="transportation", confidence=0.86),
            CategorySuggestion(category="gas", confidence=0.09),
            CategorySuggestion(category="other", confidence=0.05),
        ]

    elif any(k in merchant_upper for k in ["UBER", "LYFT", "MTA", "NJ TRANSIT"]):
        pred = "transportation"
        conf = 0.84
        top3 = [
            CategorySuggestion(category="transportation", confidence=0.84),
            CategorySuggestion(category="travel", confidence=0.10),
            CategorySuggestion(category="other", confidence=0.06),
        ]

    elif x.historical_majority_category_for_payee:
        pred = x.historical_majority_category_for_payee
        conf = 0.85
        top3 = [
            CategorySuggestion(category=pred, confidence=0.85),
            CategorySuggestion(category="groceries", confidence=0.10),
            CategorySuggestion(category="other", confidence=0.05),
        ]

    else:
        pred = "other"
        conf = 0.60
        top3 = [
            CategorySuggestion(category="other", confidence=0.60),
            CategorySuggestion(category="groceries", confidence=0.20),
            CategorySuggestion(category="restaurants", confidence=0.20),
        ]

    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category=pred,
        confidence=conf,
        top_3_suggestions=top3,
    )
