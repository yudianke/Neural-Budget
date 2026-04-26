from contextlib import asynccontextmanager
import os
import threading

from fastapi import FastAPI
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

_model_ready = False


def _update_version_gauge():
    try:
        v = real_model._model_version or "0"
        M1_MODEL_VERSION.set(int(v))
    except (ValueError, TypeError):
        M1_MODEL_VERSION.set(0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model_ready
    real_model.load()
    _model_ready = True
    _update_version_gauge()
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
    return {"status": "ok", "model_version": real_model._model_version}


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
            "/admin/reload",
        ],
    }


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------
@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    result = real_model.predict(request)
    M1_PREDICTIONS.labels(predicted_category=result.predicted_category).inc()
    M1_CONFIDENCE.observe(result.confidence)
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

    threading.Thread(target=_do_reload, args=(version,), daemon=True).start()
    return {
        "status": "reload_started",
        "pin_version": version,
        "current_version": real_model._model_version,
    }
