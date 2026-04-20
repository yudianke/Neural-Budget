"""
NeuralBudget M2 Anomaly Detection Service
FastAPI + ONNX Runtime, 4-worker Gunicorn (see gunicorn.conf.py)
Port: 8003

Model: sklearn Pipeline(StandardScaler + IsolationForest) exported to ONNX
Features (6, in order): abs_amount, repeat_count, is_recurring_candidate,
                         user_txn_index, user_mean_abs_amount_prior, user_std_abs_amount_prior

Close-the-loop endpoints:
  POST /feedback                           — log dismiss / confirm feedback
  GET  /metrics/feedback                   — aggregate feedback stats
  GET  /metrics/feedback/since/{version}   — feedback stats for a model version
  POST /admin/reload                       — hot-reload ONNX model from disk
  GET  /health                             — includes model_version
"""

import json
import logging
import os
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Literal, Optional

import numpy as np
import onnxruntime as ort
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from prometheus_client.multiprocess import MultiProcessCollector
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONTAMINATION = float(os.environ.get("M2_CONTAMINATION", "0.05"))
DEFAULT_FEEDBACK_LOG_PATH = "/data/feedback/m2_feedback.jsonl"
# Committed model.onnx — used as fallback when MLflow is unreachable
FALLBACK_MODEL_PATH = os.path.join(os.path.dirname(__file__), "model.onnx")
# MLflow config
DEFAULT_MLFLOW_URI = "http://129.114.27.211:8000"
DEFAULT_MODEL_NAME = "m2-anomaly"
# Number of input features (must match training)
N_FEATURES = 6

logging.basicConfig(
    level=logging.INFO,
    format="[M2-SERVING %(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("m2_serving")

# ---------------------------------------------------------------------------
# Model state (mutable, protected by lock for hot-reload)
# ---------------------------------------------------------------------------
_model_lock = threading.Lock()
_session: Optional[ort.InferenceSession] = None
_input_name: str = ""
_model_version: str = "m2_isolation_forest_v1"
_use_fallback: bool = False  # True when using committed model.onnx, not MLflow


def _mlflow_reachable(uri: str) -> bool:
    """Quick TCP-level reachability check (2s timeout)."""
    from urllib.error import URLError
    from urllib.request import urlopen
    try:
        with urlopen(uri, timeout=2):
            return True
    except (URLError, TimeoutError, OSError):
        return False


def _sklearn_to_onnx_bytes(pipeline) -> bytes:
    """Convert a fitted sklearn Pipeline to ONNX bytes in memory."""
    from skl2onnx import convert_sklearn
    from skl2onnx.common.data_types import FloatTensorType
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        onnx_model = convert_sklearn(
            pipeline,
            name="m2_anomaly_pipeline",
            initial_types=[("float_input", FloatTensorType([None, N_FEATURES]))],
            target_opset={"": 17, "ai.onnx.ml": 3},
        )
    return onnx_model.SerializeToString()


def _load_pipeline_via_rest(tracking_uri: str, logged_model_id: str) -> object:
    """Download model.pkl directly via MLflow REST API — no S3 credentials needed.

    MLflow proxies artifact downloads through its own HTTP API, so this works
    even when the S3 backend is inaccessible from the serving container.
    Format: GET /ajax-api/2.0/mlflow/logged-models/{id}/artifacts/files?artifact_file_path=model.pkl
    """
    import tempfile
    import joblib
    import urllib.request

    url = (
        f"{tracking_uri.rstrip('/')}/ajax-api/2.0/mlflow/logged-models"
        f"/{logged_model_id}/artifacts/files?artifact_file_path=model.pkl"
    )
    log.info(f"REST download: {url}")
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        tmp_path = f.name
    urllib.request.urlretrieve(url, tmp_path)
    size_kb = os.path.getsize(tmp_path) / 1024
    log.info(f"Downloaded model.pkl via REST ({size_kb:.0f} KB)")
    pipeline = joblib.load(tmp_path)
    os.unlink(tmp_path)
    return pipeline


def _load_from_mlflow() -> Optional[str]:
    """Download latest M2 model from MLflow registry, convert to ONNX, return version string.

    Two download strategies (in order):
      1. mlflow.sklearn.load_model() via S3 — works when AWS credentials are set
      2. REST API download via MLflow HTTP proxy — works without S3 credentials

    Returns the version string on success, None on failure.
    Fail-open: any exception is caught and logged — caller falls back to disk model.
    """
    import mlflow
    import mlflow.sklearn
    from mlflow.tracking import MlflowClient

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_URI)
    model_name = os.environ.get("M2_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME)
    # Hardcoded logged model ID from export_to_onnx.py — used by REST fallback
    rest_fallback_logged_model_id = os.environ.get(
        "M2_LOGGED_MODEL_ID", "m-c0a5e85dd6b0494ba3b1fa394db99480"
    )

    if not _mlflow_reachable(tracking_uri):
        log.warning(f"MLflow at {tracking_uri} unreachable — skipping MLflow pull")
        return None

    try:
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
        versions = client.search_model_versions(f"name='{model_name}'")
        if not versions:
            log.warning(f"No registered versions found for '{model_name}' — using fallback")
            return None

        latest = max(versions, key=lambda v: int(v.version))
        log.info(f"MLflow: found '{model_name}' v{latest.version} (run_id={latest.run_id})")

        # Strategy 1: standard mlflow.sklearn.load_model (needs S3 credentials)
        pipeline = None
        try:
            model_uri = f"runs:/{latest.run_id}/model"
            log.info(f"Strategy 1: downloading model from {model_uri} ...")
            pipeline = mlflow.sklearn.load_model(model_uri)
            log.info("Strategy 1 (S3) succeeded")
        except Exception as s3_err:
            log.warning(f"Strategy 1 (S3) failed: {s3_err}")

        # Strategy 2: REST API download via MLflow HTTP proxy (no S3 creds needed)
        if pipeline is None:
            try:
                log.info(f"Strategy 2: REST download, logged_model_id={rest_fallback_logged_model_id}")
                pipeline = _load_pipeline_via_rest(tracking_uri, rest_fallback_logged_model_id)
                log.info("Strategy 2 (REST) succeeded")
            except Exception as rest_err:
                log.warning(f"Strategy 2 (REST) failed: {rest_err}")

        if pipeline is None:
            log.warning("Both download strategies failed — falling back to disk model")
            return None

        log.info("Converting sklearn Pipeline to ONNX in memory ...")
        onnx_bytes = _sklearn_to_onnx_bytes(pipeline)

        sess = ort.InferenceSession(onnx_bytes, providers=["CPUExecutionProvider"])
        inp_name = sess.get_inputs()[0].name
        version_str = f"m2_isolation_forest_v{latest.version}"

        with _model_lock:
            global _session, _input_name, _model_version, _use_fallback
            _session = sess
            _input_name = inp_name
            _model_version = version_str
            _use_fallback = False

        log.info(f"Loaded MLflow model '{model_name}' version {latest.version}")
        return version_str

    except Exception as exc:
        log.warning(f"MLflow model load failed ({exc}) — falling back to disk model")
        return None


def _load_fallback() -> None:
    """Load the committed model.onnx from disk. Always succeeds or raises hard."""
    global _session, _input_name, _use_fallback
    sess = ort.InferenceSession(FALLBACK_MODEL_PATH, providers=["CPUExecutionProvider"])
    inp_name = sess.get_inputs()[0].name
    with _model_lock:
        _session = sess
        _input_name = inp_name
        _use_fallback = True
    log.warning(f"Loaded FALLBACK model from disk: {FALLBACK_MODEL_PATH}")


def _load_model(path: Optional[str] = None) -> None:
    """Load model. If path is given, load from that ONNX file directly (admin reload).
    Otherwise try MLflow first, fall back to committed model.onnx.
    """
    global _model_version, _use_fallback
    if path:
        # Explicit path (e.g. from /admin/reload with a specific file)
        sess = ort.InferenceSession(path, providers=["CPUExecutionProvider"])
        inp_name = sess.get_inputs()[0].name
        with _model_lock:
            _session = sess
            _input_name = inp_name
            _use_fallback = False
        log.info(f"Model loaded from explicit path {path}, version={_model_version}")
        return

    # Normal startup: try MLflow, fall back to disk
    version = _load_from_mlflow()
    if version is None:
        # MLflow unavailable or failed — load committed model.onnx
        _load_fallback()


_load_model()

# ---------------------------------------------------------------------------
# Prometheus custom metrics
# ---------------------------------------------------------------------------
m2_predictions_total = Counter(
    "m2_predictions_total",
    "Total anomaly scoring requests processed",
    ["badge_type"],
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
    ["rule"],
)

m2_model_version_numeric = Gauge(
    "m2_model_version_numeric",
    "Currently loaded M2 model version as integer (extracted from version string)",
)

m2_dismissals_total = Counter(
    "m2_dismissals_total",
    "Number of anomaly dismissals (false positives) logged",
    ["badge_type"],
)

m2_confirms_total = Counter(
    "m2_confirms_total",
    "Number of confirmed anomalies logged",
)

m2_dismiss_rate = Gauge(
    "m2_dismiss_rate",
    "Rolling dismiss rate (dismissals / total feedback)",
)

m2_anomaly_rate = Gauge(
    "m2_anomaly_rate",
    "Fraction of predictions flagged as anomaly",
)


def _update_version_gauge() -> None:
    match = re.search(r'v(\d+)$', _model_version)
    m2_model_version_numeric.set(int(match.group(1)) if match else 0)


_update_version_gauge()

# ---------------------------------------------------------------------------
# Prediction counters for anomaly_rate gauge
# ---------------------------------------------------------------------------
_prediction_counts = {"total": 0, "anomaly": 0}
_count_lock = threading.Lock()


def _record_prediction(is_anomaly: bool) -> None:
    with _count_lock:
        _prediction_counts["total"] += 1
        if is_anomaly:
            _prediction_counts["anomaly"] += 1
        total = _prediction_counts["total"]
        anom = _prediction_counts["anomaly"]
    if total > 0:
        m2_anomaly_rate.set(round(anom / total, 4))


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class M2Input(BaseModel):
    transaction_id: str
    synthetic_user_id: str
    abs_amount: float
    repeat_count: int = 0
    is_recurring_candidate: int = 0
    user_txn_index: int = 0
    user_mean_abs_amount_prior: float = 0.0
    user_std_abs_amount_prior: float = 1.0
    duplicate_within_24h: bool = False
    subscription_jump: bool = False
    merchant: str = ""
    date: str = ""
    m1_confidence: float = 0.0


class RuleFlags(BaseModel):
    duplicate_within_24h: bool
    subscription_jump: bool
    amount_spike: bool


class M2Output(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    transaction_id: str
    synthetic_user_id: str
    anomaly_score: float
    is_anomaly: bool
    threshold: float
    rule_flags: RuleFlags
    badge_type: Optional[str]
    model_version: str


class M2FeedbackEntry(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    transaction_id: str
    feedback_type: Literal["dismiss_false_positive", "confirmed_anomaly"]
    badge_type: Optional[str] = None
    anomaly_score: Optional[float] = None
    rule_flags: Optional[dict] = None
    merchant: Optional[str] = None
    amount: Optional[float] = None
    date: Optional[str] = None
    source: str = "actual"
    logged_at: Optional[str] = None
    model_name: Optional[str] = None
    model_version: Optional[str] = None


class M2FeedbackBatch(BaseModel):
    entries: List[M2FeedbackEntry]


# ---------------------------------------------------------------------------
# Feedback persistence
# ---------------------------------------------------------------------------
def _feedback_log_path() -> Path:
    return Path(os.environ.get("M2_FEEDBACK_LOG_PATH", DEFAULT_FEEDBACK_LOG_PATH))


def _log_feedback(entries: List[M2FeedbackEntry]) -> int:
    path = _feedback_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            payload = entry.model_dump()
            if not payload.get("logged_at"):
                payload["logged_at"] = datetime.now(timezone.utc).isoformat()
            if not payload.get("model_name"):
                payload["model_name"] = "m2-anomaly"
            if not payload.get("model_version"):
                payload["model_version"] = _model_version
            fh.write(json.dumps(payload) + "\n")
            count += 1
    return count


def _get_feedback_stats(since_version: Optional[str] = None) -> dict:
    path = _feedback_log_path()
    if not path.exists():
        return {
            "total": 0,
            "dismissals": 0,
            "confirms": 0,
            "dismiss_rate": 0.0,
            "per_rule_dismiss": {},
            "current_version": _model_version,
            "filter_version": since_version,
        }

    total, dismissals, confirms = 0, 0, 0
    rule_dismiss: dict = {}

    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if since_version and row.get("model_version") != since_version:
                continue
            total += 1
            if row.get("feedback_type") == "dismiss_false_positive":
                dismissals += 1
                bt = row.get("badge_type", "unknown")
                rule_dismiss[bt] = rule_dismiss.get(bt, 0) + 1
            elif row.get("feedback_type") == "confirmed_anomaly":
                confirms += 1

    rate = dismissals / total if total > 0 else 0.0
    return {
        "total": total,
        "dismissals": dismissals,
        "confirms": confirms,
        "dismiss_rate": round(rate, 4),
        "per_rule_dismiss": rule_dismiss,
        "current_version": _model_version,
        "filter_version": since_version,
    }


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="NeuralBudget M2 Anomaly Detection Service",
    version="2.0.0",
    description="Async anomaly scoring for transactions (IsolationForest + rules) with close-the-loop feedback.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Auto-instrument HTTP metrics. Do NOT call .expose() — custom /metrics below
# uses MultiProcessCollector to aggregate all 4 Gunicorn workers correctly.
Instrumentator().instrument(app)


# ---------------------------------------------------------------------------
# Health / info
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    if _use_fallback:
        return {
            "status": "degraded",
            "model_version": _model_version,
            "reason": "MLflow unreachable or model load failed — serving committed fallback model.onnx",
        }
    return {"status": "ok", "model_version": _model_version}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M2 Anomaly Detection Service",
        "version": "2.0.0",
        "model_version": _model_version,
        "endpoints": [
            "/health",
            "/predict/anomaly",
            "/feedback",
            "/metrics",
            "/metrics/feedback",
            "/metrics/feedback/since/{model_version}",
            "/admin/reload",
        ],
    }


# ---------------------------------------------------------------------------
# Prometheus metrics endpoint (multiprocess-safe)
# ---------------------------------------------------------------------------
@app.get("/metrics")
def metrics():
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if multiproc_dir:
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
    else:
        from prometheus_client import REGISTRY as registry  # noqa: PLC0415
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------
@app.post("/predict/anomaly", response_model=M2Output)
def predict_anomaly(x: M2Input):
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

    with _model_lock:
        outputs = _session.run(None, {_input_name: features})

    label = int(outputs[0][0])
    raw_score = outputs[1]
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

    badge_label = badge_type or "none"
    m2_predictions_total.labels(badge_type=badge_label).inc()
    m2_anomaly_score.observe(anomaly_score)
    if is_anomaly:
        m2_anomalies_total.labels(badge_type=badge_label).inc()
    if x.duplicate_within_24h:
        m2_rule_flags_total.labels(rule="duplicate_within_24h").inc()
    if x.subscription_jump:
        m2_rule_flags_total.labels(rule="subscription_jump").inc()
    _record_prediction(is_anomaly)

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
        model_version=_model_version,
    )


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------
@app.post("/feedback")
def feedback_endpoint(request: M2FeedbackBatch):
    logged = _log_feedback(request.entries)
    for entry in request.entries:
        if entry.feedback_type == "dismiss_false_positive":
            m2_dismissals_total.labels(badge_type=entry.badge_type or "unknown").inc()
        elif entry.feedback_type == "confirmed_anomaly":
            m2_confirms_total.inc()

    stats = _get_feedback_stats()
    m2_dismiss_rate.set(stats["dismiss_rate"])
    log.info(f"Feedback logged: {logged} entries, dismiss_rate={stats['dismiss_rate']:.3f}")
    return {"logged": logged, "dismiss_rate": stats["dismiss_rate"]}


# ---------------------------------------------------------------------------
# Feedback stats (used by retrain daemon)
# ---------------------------------------------------------------------------
@app.get("/metrics/feedback")
def feedback_metrics():
    return _get_feedback_stats()


@app.get("/metrics/feedback/since/{model_version}")
def feedback_metrics_since(model_version: str):
    return _get_feedback_stats(since_version=model_version)


# ---------------------------------------------------------------------------
# Admin — hot-reload ONNX model from disk
# ---------------------------------------------------------------------------
@app.post("/admin/reload")
def admin_reload(version: Optional[str] = None):
    """Hot-reload the ONNX model.

    - With no args: re-pulls the latest version from MLflow registry.
    - With ?version=N: pins to that MLflow registry version.
    - Falls back to committed model.onnx if MLflow is unreachable.
    """
    global _model_version

    def _do_reload(pin: Optional[str]) -> None:
        global _model_version
        try:
            if pin:
                # Pin to a specific version: fetch that version from MLflow
                import mlflow
                import mlflow.sklearn
                tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_URI)
                model_name = os.environ.get("M2_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME)
                mlflow.set_tracking_uri(tracking_uri)
                model_uri = f"models:/{model_name}/{pin}"
                log.info(f"Reloading pinned version {pin} from {model_uri}")
                pipeline = mlflow.sklearn.load_model(model_uri)
                onnx_bytes = _sklearn_to_onnx_bytes(pipeline)
                sess = ort.InferenceSession(onnx_bytes, providers=["CPUExecutionProvider"])
                inp_name = sess.get_inputs()[0].name
                with _model_lock:
                    global _session, _input_name, _use_fallback
                    _session = sess
                    _input_name = inp_name
                    _model_version = f"m2_isolation_forest_v{pin}"
                    _use_fallback = False
            else:
                # Pull latest from MLflow
                _load_model()
            _update_version_gauge()
            log.info(f"Hot-reload complete, version={_model_version}, fallback={_use_fallback}")
        except Exception as e:
            log.error(f"Hot-reload failed: {e}")

    threading.Thread(target=_do_reload, args=(version,), daemon=True).start()
    return {
        "status": "reload_started",
        "pin_version": version,
        "current_version": _model_version,
    }
