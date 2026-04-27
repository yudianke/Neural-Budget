from contextlib import asynccontextmanager
import json
import os
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

import real_model
from schemas import M1FeedbackBatch, M1Input, M1Output

# ---------------------------------------------------------------------------
# Custom Prometheus metrics
# ---------------------------------------------------------------------------
M1_PREDICTIONS = Counter(
    "m1_predictions_total",
    "Total M1 predictions made",
    ["predicted_category"],
)
M1_CONFIDENCE = Histogram(
    "m1_prediction_confidence",
    "Distribution of M1 prediction confidence scores",
    buckets=[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
M1_CONFIDENCE_PER_CATEGORY = Histogram(
    "m1_confidence_per_category",
    "Confidence distribution per predicted category",
    ["predicted_category"],
    buckets=[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
)
M1_LOW_CONFIDENCE_TOTAL = Counter(
    "m1_low_confidence_predictions_total",
    "Predictions with confidence below auto-fill threshold (0.6)",
)
M1_AUTO_FILL_TOTAL = Counter(
    "m1_auto_fill_total",
    "Predictions that triggered auto-fill (confidence >= 0.6)",
)
M1_CORRECTIONS = Counter(
    "m1_corrections_total",
    "Number of times user overrode the ML-predicted category",
)
M1_CORRECTION_TRANSITIONS = Counter(
    "m1_correction_transitions_total",
    "User overrides from predicted category to chosen category",
    ["predicted_category", "chosen_category"],
)
M1_ACCEPTS = Counter(
    "m1_accepts_total",
    "Number of times user accepted the ML-predicted category",
)
M1_CORRECTION_RATE = Gauge(
    "m1_correction_rate",
    "Rolling correction rate (overrides / total feedback)",
)
M1_MODEL_VERSION = Gauge(
    "m1_model_version_numeric",
    "Currently loaded model version as an integer (0 if non-numeric)",
)
M1_EVAL_MACRO_F1 = Gauge(
    "m1_eval_macro_f1",
    "Offline eval macro F1 of the currently loaded model",
)
M1_EVAL_ACCURACY = Gauge(
    "m1_eval_accuracy",
    "Offline eval accuracy of the currently loaded model",
)
M1_EVAL_GATE_PASSED = Gauge(
    "m1_eval_gate_passed",
    "1 if offline eval gate passed for the current model, 0 if failed",
)
M1_EVAL_CATEGORY_F1 = Gauge(
    "m1_eval_category_f1",
    "Per-category F1 from offline eval",
    ["category"],
)

_model_ready = False
CONFIDENCE_THRESHOLD = 0.6
INFERENCE_LOG_PATH = Path(os.getenv("M1_INFERENCE_LOG_PATH", "/data/feedback/m1_inference_log.jsonl"))
REJECTED_LOG_PATH = Path(os.getenv("M1_REJECTED_LOG_PATH", "/data/feedback/m1_rejected_samples.jsonl"))


def _append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def _validate_input_for_prediction(request: M1Input) -> None:
    merchant = str(request.merchant).strip()

    if not merchant:
        _append_jsonl(REJECTED_LOG_PATH, {
            "timestamp": time.time(),
            "reason": "empty_merchant",
            "transaction_id": request.transaction_id,
            "synthetic_user_id": request.synthetic_user_id,
            "merchant": request.merchant,
            "amount": request.amount,
        })
        raise HTTPException(status_code=400, detail="merchant cannot be empty")

    if request.amount == 0:
        _append_jsonl(REJECTED_LOG_PATH, {
            "timestamp": time.time(),
            "reason": "zero_amount",
            "transaction_id": request.transaction_id,
            "synthetic_user_id": request.synthetic_user_id,
            "merchant": request.merchant,
            "amount": request.amount,
        })
        raise HTTPException(status_code=400, detail="amount cannot be zero")

    if abs(request.amount) > 100000:
        _append_jsonl(REJECTED_LOG_PATH, {
            "timestamp": time.time(),
            "reason": "amount_out_of_range",
            "transaction_id": request.transaction_id,
            "synthetic_user_id": request.synthetic_user_id,
            "merchant": request.merchant,
            "amount": request.amount,
        })
        raise HTTPException(status_code=400, detail="amount is out of allowed range")


def _update_version_gauge():
    try:
        v = real_model._model_version or "0"
        M1_MODEL_VERSION.set(int(v))
    except (ValueError, TypeError):
        M1_MODEL_VERSION.set(0)


def _update_eval_gauges():
    results = real_model.get_eval_results()
    if not results:
        return

    M1_EVAL_GATE_PASSED.set(1.0 if results.get("gate_passed", True) else 0.0)

    evaluations = results.get("evaluations", {})
    for source in ("synthetic", "sanity"):
        if source in evaluations:
            source_metrics = evaluations[source]
            M1_EVAL_MACRO_F1.set(source_metrics.get("macro_f1", 0.0))
            M1_EVAL_ACCURACY.set(source_metrics.get("accuracy", 0.0))
            for category, f1_value in source_metrics.get("per_category_f1", {}).items():
                M1_EVAL_CATEGORY_F1.labels(category=category).set(f1_value)
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model_ready
    real_model.load()
    _model_ready = True
    _update_version_gauge()
    _update_eval_gauges()
    yield


app = FastAPI(
    title="NeuralBudget M1 Baseline Service",
    version="1.0.0",
    description="Baseline FastAPI service for M1 transaction auto-categorization.",
    lifespan=lifespan,
)

# Instrument all HTTP endpoints with default latency/request metrics at /metrics
Instrumentator().instrument(app).expose(app)


# ---------------------------------------------------------------------------
# CORS + COEP middleware
# ---------------------------------------------------------------------------
class CrossOriginMiddleware(BaseHTTPMiddleware):
    """Handle CORS + COEP so browsers with require-corp allow the response."""

    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "*")

        if request.method == "OPTIONS":
            return Response(
                content="OK",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Max-Age": "600",
                    "Cross-Origin-Resource-Policy": "cross-origin",
                },
            )

        response: Response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
        return response


app.add_middleware(CrossOriginMiddleware)


# ---------------------------------------------------------------------------
# Health / info
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    if not _model_ready:
        return {"status": "loading"}
    if real_model._use_fallback and real_model._fallback_mode == "degraded":
        return {
            "status": "degraded",
            "model_version": None,
            "reason": "MLflow unreachable or model load failed — serving null predictions",
        }
    eval_results = real_model.get_eval_results()
    eval_gate = eval_results.get("gate_passed", True) if eval_results else True
    return {
        "status": "ok",
        "model_version": real_model._model_version,
        "eval_gate_passed": eval_gate,
    }


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M1 Baseline Service",
        "version": "1.0.0",
        "model": real_model.get_model_info(),
        "endpoints": [
            "/health",
            "/predict/category",
            "/feedback",
            "/metrics",
            "/metrics/feedback",
            "/metrics/feedback/since/{model_version}",
            "/metrics/evaluation",
            "/admin/reload",
            "/admin/run-eval",
        ],
    }


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------
@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    _validate_input_for_prediction(request)

    result = real_model.predict(request)

    M1_PREDICTIONS.labels(predicted_category=result.predicted_category).inc()
    M1_CONFIDENCE.observe(result.confidence)
    if result.predicted_category:
        M1_CONFIDENCE_PER_CATEGORY.labels(
            predicted_category=result.predicted_category
        ).observe(result.confidence)
    if result.confidence < CONFIDENCE_THRESHOLD:
        M1_LOW_CONFIDENCE_TOTAL.inc()
    if result.auto_fill:
        M1_AUTO_FILL_TOTAL.inc()

    _append_jsonl(INFERENCE_LOG_PATH, {
        "timestamp": time.time(),
        "stage": "m1_inference",
        "transaction_id": request.transaction_id,
        "synthetic_user_id": request.synthetic_user_id,
        "merchant": request.merchant,
        "amount": request.amount,
        "transaction_type": request.transaction_type,
        "account_type": request.account_type,
        "day_of_week": request.day_of_week,
        "day_of_month": request.day_of_month,
        "month": request.month,
        "log_abs_amount": request.log_abs_amount,
        "historical_majority_category_for_payee": request.historical_majority_category_for_payee,
        "predicted_category": result.predicted_category,
        "confidence": result.confidence,
        "top_3_suggestions": [x.model_dump() for x in result.top_3_suggestions],
        "auto_fill": result.auto_fill,
        "model_version": real_model._model_version,
    })

    return result


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------
@app.post("/feedback")
def feedback_endpoint(request: M1FeedbackBatch):
    logged = real_model.log_feedback(request.entries)
    for entry in request.entries:
        if entry.feedback_type == "overridden":
            M1_CORRECTIONS.inc()
            M1_CORRECTION_TRANSITIONS.labels(
                predicted_category=entry.predicted_category or "unknown",
                chosen_category=entry.chosen_category or "unknown",
            ).inc()
        elif entry.feedback_type == "accepted":
            M1_ACCEPTS.inc()

    # Update rolling correction rate gauge
    stats = real_model.get_feedback_stats()
    M1_CORRECTION_RATE.set(stats["correction_rate"])
    return {"logged": logged}


# ---------------------------------------------------------------------------
# Feedback stats (used by retrain-daemon)
# ---------------------------------------------------------------------------
@app.get("/metrics/feedback")
def feedback_metrics():
    """Total feedback stats — used by retrain-daemon to count corrections."""
    return real_model.get_feedback_stats()


@app.get("/metrics/feedback/since/{model_version}")
def feedback_metrics_since(model_version: str):
    """Feedback stats filtered to a specific model version — used for rollback evaluation."""
    return real_model.get_feedback_stats(since_version=model_version)


@app.get("/metrics/evaluation")
def evaluation_metrics():
    """Return the latest offline evaluation results for the loaded model."""
    results = real_model.get_eval_results()
    if not results:
        return {"status": "no evaluation run yet"}
    return results


# ---------------------------------------------------------------------------
# Admin — hot-reload model
# ---------------------------------------------------------------------------
@app.post("/admin/reload")
def admin_reload(version: str | None = None):
    """Hot-reload a model version from MLflow.

    Args:
        version: Optional MLflow model version to pin. If omitted, loads the
                 latest registered version. If provided, sets M1_MODEL_VERSION
                 env var before reloading so _select_model_version() picks
                 the correct version — this is how rollback works.

    Called by retrain-daemon after a new version is registered (version=None)
    or when rolling back to a previous version (version=<old_version>).
    """
    def _do_reload(pin: str | None):
        if pin is not None:
            os.environ["M1_MODEL_VERSION"] = str(pin)
        elif "M1_MODEL_VERSION" in os.environ:
            # Clear any previous pin so we load the latest
            del os.environ["M1_MODEL_VERSION"]
        real_model.reload()
        _update_version_gauge()
        _update_eval_gauges()

    threading.Thread(target=_do_reload, args=(version,), daemon=True).start()
    return {
        "status": "reload_started",
        "pin_version": version,
        "current_version": real_model._model_version,
    }


@app.post("/admin/run-eval")
def admin_run_eval():
    """Re-run offline evaluation on the currently loaded model."""
    results = real_model.rerun_eval()
    _update_eval_gauges()
    return results
