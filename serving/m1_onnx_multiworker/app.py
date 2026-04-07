from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import math
import numpy as np
import onnxruntime as ort


app = FastAPI(title="NeuralBudget M1 ONNX Service")


class CategorySuggestion(BaseModel):
    category: str
    confidence: float


class M1Input(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    date: str
    merchant: str
    amount: float
    transaction_type: str
    account_type: str
    day_of_week: int
    day_of_month: int
    month: int
    log_abs_amount: float
    historical_majority_category_for_payee: str


class M1Output(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    predicted_category: str
    confidence: float
    top_3_suggestions: List[CategorySuggestion]


CATEGORY_NAMES = ["restaurants", "groceries", "other"]

session = ort.InferenceSession("model.onnx", providers=["CPUExecutionProvider"])


def merchant_feature(merchant: str) -> float:
    merchant_upper = merchant.upper()
    if "SUBWAY" in merchant_upper or "MCDONALD" in merchant_upper or "BURGER" in merchant_upper:
        return 1.0
    if "TESCO" in merchant_upper or "WALMART" in merchant_upper or "TARGET" in merchant_upper:
        return 0.5
    return 0.0


def hist_cat_feature(cat: str) -> float:
    mapping = {
        "restaurants": 1.0,
        "groceries": 0.5,
        "other": 0.0
    }
    return mapping.get(cat.lower(), 0.0) if cat else 0.0


def softmax(x: np.ndarray) -> np.ndarray:
    z = x - np.max(x, axis=1, keepdims=True)
    exp = np.exp(z)
    return exp / np.sum(exp, axis=1, keepdims=True)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict/category", response_model=M1Output)
def predict(x: M1Input):
    features = np.array([[
        float(x.log_abs_amount),
        float(x.day_of_week),
        merchant_feature(x.merchant),
        hist_cat_feature(x.historical_majority_category_for_payee)
    ]], dtype=np.float32)

    logits = session.run(["logits"], {"features": features})[0]
    probs = softmax(logits)[0]

    ranked_idx = np.argsort(-probs)
    pred_idx = int(ranked_idx[0])

    top3 = [
        {
            "category": CATEGORY_NAMES[int(i)],
            "confidence": float(round(probs[int(i)], 4))
        }
        for i in ranked_idx[:3]
    ]

    return {
        "transaction_id": x.transaction_id,
        "synthetic_user_id": x.synthetic_user_id,
        "predicted_category": CATEGORY_NAMES[pred_idx],
        "confidence": float(round(probs[pred_idx], 4)),
        "top_3_suggestions": top3
    }
