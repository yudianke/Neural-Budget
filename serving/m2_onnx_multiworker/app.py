"""
NeuralBudget M2 Anomaly Detection Service
FastAPI + ONNX Runtime, 4-worker Gunicorn (see gunicorn.conf.py)
Port: 8002

Model: sklearn Pipeline(StandardScaler + IsolationForest) exported to ONNX
Features (6, in order): abs_amount, repeat_count, is_recurring_candidate,
                         user_txn_index, user_mean_abs_amount_prior, user_std_abs_amount_prior
"""
import os
from typing import Optional

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# contamination used at train time (config_m2.yaml); only informational in output
CONTAMINATION = float(os.environ.get("M2_CONTAMINATION", "0.05"))
MODEL_VERSION = "m2_isolation_forest_v1"

_model_path = os.path.join(os.path.dirname(__file__), "model.onnx")
session = ort.InferenceSession(_model_path, providers=["CPUExecutionProvider"])
_input_name = session.get_inputs()[0].name


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class M2Input(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    # --- 6 model features (required) ---
    abs_amount: float
    repeat_count: int = 0
    is_recurring_candidate: int = 0       # 0 or 1
    user_txn_index: int = 0               # COUNT(*) of user's prior transactions
    user_mean_abs_amount_prior: float = 0.0
    user_std_abs_amount_prior: float = 1.0
    # --- deterministic rule inputs computed in loot-core ---
    duplicate_within_24h: bool = False
    subscription_jump: bool = False
    # --- optional context (not fed to model) ---
    merchant: str = ""
    date: str = ""
    m1_confidence: float = 0.0


class RuleFlags(BaseModel):
    duplicate_within_24h: bool
    subscription_jump: bool
    amount_spike: bool


class M2Output(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    anomaly_score: float
    is_anomaly: bool
    threshold: float
    rule_flags: RuleFlags
    badge_type: Optional[str]
    model_version: str


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NeuralBudget M2 Anomaly Detection Service",
    version="1.0.0",
    description="Async anomaly scoring for transactions (IsolationForest + rules).",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M2 Anomaly Detection Service",
        "version": "1.0.0",
        "endpoints": ["/health", "/predict/anomaly"],
    }


@app.post("/predict/anomaly", response_model=M2Output)
def predict_anomaly(x: M2Input):
    # Build feature vector in exact FEATURE_COLS order from train_m2.py
    features = np.array(
        [[
            x.abs_amount,
            float(x.repeat_count),
            float(x.is_recurring_candidate),
            float(x.user_txn_index),
            x.user_mean_abs_amount_prior,
            x.user_std_abs_amount_prior,
        ]],
        dtype=np.float32,
    )

    outputs = session.run(None, {_input_name: features})
    # outputs[0]: label array (int64), -1=anomaly, 1=normal
    # outputs[1]: scores array (float), decision_function values
    label = int(outputs[0][0])
    raw_score = outputs[1]
    # scores may be a 1-D array or a list of length 1
    anomaly_score = float(raw_score[0]) if hasattr(raw_score, "__len__") else float(raw_score)

    is_ml_anomaly = label == -1
    amount_spike = is_ml_anomaly and not x.duplicate_within_24h and not x.subscription_jump
    is_anomaly = is_ml_anomaly or x.duplicate_within_24h or x.subscription_jump

    if x.duplicate_within_24h:
        badge_type = "duplicate"
    elif x.subscription_jump:
        badge_type = "price_jump"
    elif amount_spike:
        badge_type = "spike"
    else:
        badge_type = None

    return M2Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        anomaly_score=anomaly_score,
        is_anomaly=is_anomaly,
        threshold=CONTAMINATION,
        rule_flags=RuleFlags(
            duplicate_within_24h=x.duplicate_within_24h,
            subscription_jump=x.subscription_jump,
            amount_spike=amount_spike,
        ),
        badge_type=badge_type,
        model_version=MODEL_VERSION,
    )
