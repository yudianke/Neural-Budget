"""
NeuralBudget M2 Anomaly Detection Service
FastAPI + ONNX Runtime, 4-worker Gunicorn (see gunicorn.conf.py)
Port: 8003

Model: sklearn Pipeline(StandardScaler + IsolationForest) exported to ONNX
Features (6, in order): abs_amount, repeat_count, is_recurring_candidate,
                         user_txn_index, user_mean_abs_amount_prior, user_std_abs_amount_prior
"""
import os
import re
from typing import Optional

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
)
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

# contamination used at train time (config_m2.yaml); only informational in output
CONTAMINATION = float(os.environ.get("M2_CONTAMINATION", "0.05"))
MODEL_VERSION = "m2_isolation_forest_v1"

_model_path = os.path.join(os.path.dirname(__file__), "model.onnx")
session = ort.InferenceSession(_model_path, providers=["CPUExecutionProvider"])
_input_name = session.get_inputs()[0].name

# ---------------------------------------------------------------------------
# Prometheus custom metrics  (mirrors the M1 pattern)
# ---------------------------------------------------------------------------

m2_predictions_total = Counter(
    "m2_predictions_total",
    "Total anomaly scoring requests processed",
    ["badge_type"],           # "duplicate", "price_jump", "spike", "none"
)

m2_anomalies_total = Counter(
    "m2_anomalies_total",
    "Total transactions flagged as anomalies (is_anomaly=true)",
    ["badge_type"],
)

m2_anomaly_score = Histogram(
    "m2_anomaly_score",
    "Distribution of raw IsolationForest decision-function scores",
    buckets=[-0.5, -0.3, -0.1, 0.0, 0.05, 0.1, 0.2, 0.3, 0.5],
)

m2_rule_flags_total = Counter(
    "m2_rule_flags_total",
    "Count of deterministic rule triggers",
    ["rule"],                  # "duplicate_within_24h", "subscription_jump"
)

m2_model_version_numeric = Gauge(
    "m2_model_version_numeric",
    "Currently loaded M2 model version as integer (extracted from version string)",
)


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

# Auto-instrument HTTP metrics (http_requests_total, http_request_duration_seconds).
# Do NOT call .expose() here — we provide a custom /metrics endpoint below that
# uses MultiProcessCollector so all 4 Gunicorn workers are aggregated correctly.
Instrumentator().instrument(app)

# Set model version gauge — extract only the trailing version number after 'v'
# e.g. "m2_isolation_forest_v1" -> 1  (NOT "21" from naive digit strip)
_match = re.search(r'v(\d+)$', MODEL_VERSION)
m2_model_version_numeric.set(int(_match.group(1)) if _match else 0)


@app.get("/health")
def health():
    return {"status": "ok", "model_version": MODEL_VERSION}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M2 Anomaly Detection Service",
        "version": "1.0.0",
        "model_version": MODEL_VERSION,
        "endpoints": ["/health", "/predict/anomaly", "/metrics"],
    }


@app.get("/metrics")
def metrics():
    """Prometheus scrape endpoint — aggregates all Gunicorn worker processes.

    Requires PROMETHEUS_MULTIPROC_DIR to be set to a shared writable directory
    (configured in docker-compose.yml and gunicorn.conf.py child_exit hook).
    Falls back to single-process registry if the env var is absent (local dev).
    """
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if multiproc_dir:
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
    else:
        # Local dev without Gunicorn: use the default global registry
        from prometheus_client import REGISTRY as registry  # noqa: PLC0415
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


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

    # --- record Prometheus metrics ---
    badge_label = badge_type or "none"
    m2_predictions_total.labels(badge_type=badge_label).inc()
    m2_anomaly_score.observe(anomaly_score)
    if is_anomaly:
        m2_anomalies_total.labels(badge_type=badge_label).inc()
    if x.duplicate_within_24h:
        m2_rule_flags_total.labels(rule="duplicate_within_24h").inc()
    if x.subscription_jump:
        m2_rule_flags_total.labels(rule="subscription_jump").inc()

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
