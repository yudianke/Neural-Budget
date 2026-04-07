from ray import serve
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import numpy as np
import onnxruntime as ort


app = FastAPI(title="NeuralBudget M1 Ray Serve Bonus")


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


CATEGORY_NAMES = ["restaurants", "groceries", "other"]


@serve.deployment(num_replicas=2)
@serve.ingress(app)
class RayM1Service:
    def __init__(self):
        self.session = ort.InferenceSession(
            "model.onnx",
            providers=["CPUExecutionProvider"]
        )

    def merchant_feature(self, merchant: str) -> float:
        merchant = merchant.upper()
        if "SUBWAY" in merchant:
            return 1.0
        if "WALMART" in merchant:
            return 0.5
        return 0.0

    @app.get("/health")
    def health(self):
        return {"status": "ok"}

    @app.post("/predict/category")
    def predict(self, x: M1Input):
        features = np.array([[
            float(x.log_abs_amount),
            float(x.day_of_week),
            self.merchant_feature(x.merchant),
            1.0
        ]], dtype=np.float32)

        logits = self.session.run(["logits"], {"features": features})[0]
        pred_idx = int(np.argmax(logits))

        return {
            "transaction_id": x.transaction_id,
            "synthetic_user_id": x.synthetic_user_id,
            "predicted_category": CATEGORY_NAMES[pred_idx],
            "confidence": 0.95,
            "top_3_suggestions": [
                {"category": "restaurants", "confidence": 0.95},
                {"category": "groceries", "confidence": 0.03},
                {"category": "other", "confidence": 0.02}
            ]
        }


deployment = RayM1Service.bind()
