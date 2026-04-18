from schemas import M1Input, M1Output, CategorySuggestion

# Project categories (must match names in the migration 1776000000000_add_m1_categories.js)
CATEGORY_RESTAURANTS = "Restaurants"
CATEGORY_GROCERIES = "Groceries"
CATEGORY_GAS = "Gas"
CATEGORY_TRANSPORT = "Transport"
CATEGORY_SHOPPING = "Shopping"
CATEGORY_HEALTHCARE = "Healthcare"
CATEGORY_ENTERTAINMENT = "Entertainment"
CATEGORY_UTILITIES = "Utilities"
CATEGORY_HOUSING = "Housing"
CATEGORY_MISC = "Misc"


def predict_category(x: M1Input) -> M1Output:
    merchant_upper = x.merchant.upper().strip()

    if any(k in merchant_upper for k in ["SUBWAY", "MCDONALD", "BURGER", "KFC", "PIZZA", "NANDOS", "GREGGS", "STARBUCKS", "COSTA", "PRET"]):
        pred, conf = CATEGORY_RESTAURANTS, 0.91
        top3 = [
            CategorySuggestion(category=CATEGORY_RESTAURANTS, confidence=0.91),
            CategorySuggestion(category=CATEGORY_GROCERIES, confidence=0.05),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.04),
        ]
    elif any(k in merchant_upper for k in ["TESCO", "SAINSBURY", "ASDA", "MORRISONS", "LIDL", "ALDI", "WALMART", "WHOLE FOODS", "TRADER JOE", "COSTCO", "WAITROSE"]):
        pred, conf = CATEGORY_GROCERIES, 0.88
        top3 = [
            CategorySuggestion(category=CATEGORY_GROCERIES, confidence=0.88),
            CategorySuggestion(category=CATEGORY_SHOPPING, confidence=0.07),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.05),
        ]
    elif any(k in merchant_upper for k in ["SHELL", "EXXON", "BP ", "CHEVRON", "TEXACO", "GULF", "ESSO", "PETRO"]):
        pred, conf = CATEGORY_GAS, 0.86
        top3 = [
            CategorySuggestion(category=CATEGORY_GAS, confidence=0.86),
            CategorySuggestion(category=CATEGORY_TRANSPORT, confidence=0.09),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.05),
        ]
    elif any(k in merchant_upper for k in ["UBER", "LYFT", "TFL", "TRAINLINE", "NATIONAL RAIL", "BUS", "TAXI"]):
        pred, conf = CATEGORY_TRANSPORT, 0.84
        top3 = [
            CategorySuggestion(category=CATEGORY_TRANSPORT, confidence=0.84),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.10),
            CategorySuggestion(category=CATEGORY_GAS, confidence=0.06),
        ]
    elif any(k in merchant_upper for k in ["AMAZON", "EBAY", "ASOS", "H&M", "ZARA", "PRIMARK", "MARKS"]):
        pred, conf = CATEGORY_SHOPPING, 0.82
        top3 = [
            CategorySuggestion(category=CATEGORY_SHOPPING, confidence=0.82),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.12),
            CategorySuggestion(category=CATEGORY_GROCERIES, confidence=0.06),
        ]
    elif any(k in merchant_upper for k in ["NETFLIX", "SPOTIFY", "CINEMA", "VUE", "ODEON", "DISNEY", "AMAZON PRIME", "YOUTUBE"]):
        pred, conf = CATEGORY_ENTERTAINMENT, 0.85
        top3 = [
            CategorySuggestion(category=CATEGORY_ENTERTAINMENT, confidence=0.85),
            CategorySuggestion(category=CATEGORY_SHOPPING, confidence=0.10),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.05),
        ]
    elif any(k in merchant_upper for k in ["PHARMACY", "BOOTS", "CHEMIST", "NHS", "HOSPITAL", "DENTIST", "DOCTOR", "SUPERDRUG"]):
        pred, conf = CATEGORY_HEALTHCARE, 0.83
        top3 = [
            CategorySuggestion(category=CATEGORY_HEALTHCARE, confidence=0.83),
            CategorySuggestion(category=CATEGORY_SHOPPING, confidence=0.12),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.05),
        ]
    elif any(k in merchant_upper for k in ["GAS", "ELECTRIC", "WATER", "BT ", "SKY ", "BROADBAND", "VIRGIN MEDIA", "EDF", "E.ON", "BRITISH GAS"]):
        pred, conf = CATEGORY_UTILITIES, 0.87
        top3 = [
            CategorySuggestion(category=CATEGORY_UTILITIES, confidence=0.87),
            CategorySuggestion(category=CATEGORY_HOUSING, confidence=0.09),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.04),
        ]
    elif any(k in merchant_upper for k in ["RENT", "MORTGAGE", "ESTATE", "LETTING", "LANDLORD"]):
        pred, conf = CATEGORY_HOUSING, 0.92
        top3 = [
            CategorySuggestion(category=CATEGORY_HOUSING, confidence=0.92),
            CategorySuggestion(category=CATEGORY_UTILITIES, confidence=0.05),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.03),
        ]
    elif x.historical_majority_category_for_payee:
        pred = x.historical_majority_category_for_payee
        conf = 0.75
        top3 = [
            CategorySuggestion(category=pred, confidence=0.75),
            CategorySuggestion(category=CATEGORY_GROCERIES, confidence=0.15),
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.10),
        ]
    else:
        pred, conf = CATEGORY_MISC, 0.55
        top3 = [
            CategorySuggestion(category=CATEGORY_MISC, confidence=0.55),
            CategorySuggestion(category=CATEGORY_GROCERIES, confidence=0.25),
            CategorySuggestion(category=CATEGORY_RESTAURANTS, confidence=0.20),
        ]

    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category=pred,
        confidence=conf,
        top_3_suggestions=top3,
    )
