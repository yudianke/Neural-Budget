"""Local dev server using mock_model — no MLflow/XGBoost needed.

Mirrors the same API surface as server.py (production) so local testing
reflects what runs on the VM. Skips Prometheus instrumentation to keep
local dependencies minimal.
"""
import json
from pathlib import Path

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

import mock_model
from schemas import M1FeedbackBatch, M1Input, M1Output

FEEDBACK_LOG = Path("/tmp/m1_local_feedback.jsonl")

app = FastAPI(title="NeuralBudget M1 (local mock)", version="1.0.0")


class CrossOriginMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "*")
        if request.method == "OPTIONS":
            return Response(
                content="OK",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
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
    return {"status": "ok", "mode": "local-mock", "model_version": "mock"}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M1 (local mock)",
        "endpoints": [
            "/health", "/predict/category", "/feedback",
            "/metrics/feedback", "/metrics/feedback/since/{model_version}",
            "/admin/reload",
        ],
    }


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------
@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    result = mock_model.predict_category(request)
    print(f"[M1-local] {request.merchant!r} -> {result.predicted_category} ({result.confidence:.0%})")
    return result


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------
@app.post("/feedback")
def feedback_endpoint(request: M1FeedbackBatch):
    with FEEDBACK_LOG.open("a") as f:
        for e in request.entries:
            f.write(json.dumps(e.model_dump()) + "\n")
    for e in request.entries:
        print(f"[M1-feedback] {e.merchant!r} predicted={e.predicted_category} "
              f"chosen={e.chosen_category} type={e.feedback_type}")
    return {"logged": len(request.entries)}


# ---------------------------------------------------------------------------
# Feedback stats (same interface as production)
# ---------------------------------------------------------------------------
@app.get("/metrics/feedback")
def feedback_metrics():
    return _get_stats()


@app.get("/metrics/feedback/since/{model_version}")
def feedback_metrics_since(model_version: str):
    return _get_stats(since_version=model_version)


def _get_stats(since_version: str | None = None) -> dict:
    total, corrections = 0, 0
    if FEEDBACK_LOG.exists():
        for line in FEEDBACK_LOG.read_text().splitlines():
            try:
                row = json.loads(line)
            except Exception:
                continue
            if since_version and row.get("model_version") != since_version:
                continue
            total += 1
            if row.get("feedback_type") == "overridden":
                corrections += 1
    rate = corrections / total if total > 0 else 0.0
    return {
        "total": total,
        "corrections": corrections,
        "correction_rate": round(rate, 4),
        "current_version": "mock",
        "filter_version": since_version,
    }


# ---------------------------------------------------------------------------
# Admin reload (no-op locally, mirrors production API)
# ---------------------------------------------------------------------------
@app.post("/admin/reload")
def admin_reload(version: str | None = None):
    """Mirror of production endpoint. In local mock, just logs — no MLflow to call."""
    if version:
        print(f"[M1-local] /admin/reload?version={version} called (mock pins to {version})")
    else:
        print("[M1-local] /admin/reload called — would load latest version (mock)")
    return {"status": "reload_started", "pin_version": version, "current_version": "mock"}
