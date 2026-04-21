"""
M3 Forecast Inference Service

Loads the latest registered m3-forecast bundle from MLflow at startup
(same pattern as M1's real_model.py). Exposes /forecast/features for the
ActualBudget backend worker to call.

Environment variables:
    MLFLOW_TRACKING_URI         MLflow server URL
    M3_REGISTERED_MODEL         MLflow model name (default: m3-forecast)
    M3_MODEL_VERSION            Pin a specific version (optional, default: latest)
    M3_FORECAST_LOG_PATH        Path to forecast JSONL log (default: /data/m3_feedback/m3_forecasts.jsonl)
    M3_ACTUALS_URL              URL of ActualBudget export endpoint for forecast-accuracy (optional)

Endpoints:
    GET  /health                         Service health + loaded model version
    POST /forecast/features              Real-time forecast from feature rows
    GET  /metrics                        Prometheus metrics
    GET  /metrics/forecast-accuracy      Per-category MAE vs actuals for a model version
"""
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import joblib
import mlflow
import numpy as np
import pandas as pd
import requests as http_requests
from fastapi import FastAPI, Query
from mlflow.tracking import MlflowClient
from prometheus_client import Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from forecast_log import log_forecasts, compute_mae_vs_actuals

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("m3_service")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MLFLOW_URI   = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.26.214:8000")
MODEL_NAME   = os.environ.get("M3_REGISTERED_MODEL", "m3-forecast")
MODEL_VER    = os.environ.get("M3_MODEL_VERSION")   # None = latest
# URL to the ActualBudget monthly-history export (called by /metrics/forecast-accuracy).
# When set, the endpoint fetches real actuals from ActualBudget to compute in-production MAE.
ACTUALS_URL  = os.environ.get("M3_ACTUALS_URL", "")

# ---------------------------------------------------------------------------
# Model globals (populated by load())
# ---------------------------------------------------------------------------
_model         = None
_feature_cols: list[str] = []
_model_name    = MODEL_NAME
_model_version: str | None = None
_degraded      = False

# ---------------------------------------------------------------------------
# Prometheus custom metrics
# ---------------------------------------------------------------------------
M3_PREDICTIONS = Counter("m3_predictions_total", "M3 forecast requests served", ["n_categories"])
M3_LATENCY     = Histogram("m3_forecast_latency_seconds", "Forecast request latency")
M3_MODEL_VER   = Gauge("m3_model_version_numeric", "Loaded model version (int)")


# ---------------------------------------------------------------------------
# Load from MLflow
# ---------------------------------------------------------------------------
def _select_version(client: MlflowClient) -> str | None:
    # Read live so /admin/reload?version=X actually works (not frozen at import)
    pin = os.environ.get("M3_MODEL_VERSION")
    if pin:
        return pin
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    if not versions:
        return None
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    return str(latest.version)


def load() -> None:
    global _model, _feature_cols, _model_name, _model_version, _degraded
    logger.info("Loading M3 model from MLflow: %s @ %s", MODEL_NAME, MLFLOW_URI)
    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        ver = _select_version(client)
        if ver is None:
            raise RuntimeError(f"No registered versions for {MODEL_NAME}")

        ver_info = client.get_model_version(MODEL_NAME, ver)
        bundle_dir = mlflow.artifacts.download_artifacts(
            run_id=ver_info.run_id, artifact_path="bundle"
        )
        import glob
        bundle_files = glob.glob(f"{bundle_dir}/*.joblib")
        if not bundle_files:
            raise RuntimeError(f"No .joblib bundle found in {bundle_dir}")

        bundle = joblib.load(bundle_files[0])
        _model         = bundle["model"]
        _feature_cols  = bundle["feature_columns"]
        _model_name    = bundle.get("model_name", MODEL_NAME)
        _model_version = ver
        _degraded      = False

        try:
            M3_MODEL_VER.set(int(ver))
        except (ValueError, TypeError):
            M3_MODEL_VER.set(0)

        logger.info("Loaded %s v%s (%d features)", MODEL_NAME, ver, len(_feature_cols))
    except Exception as e:
        _degraded = True
        _model_version = None
        logger.error("M3 model load failed — degraded mode: %s", e)


def reload() -> None:
    logger.info("reload() triggered")
    load()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class CategoryForecast(BaseModel):
    category: str
    forecast: Optional[float]


class ForecastFeatureRow(BaseModel):
    """Feature row computed from ActualBudget transaction history.

    All fields must be computable at inference time from transaction data.
    User-level demographic features (persona_cluster, AGE_REF, etc.) are
    intentionally excluded — they are not available from real budget data.
    """
    project_category: str
    monthly_spend: float = 0.0
    lag_1: float = 0.0
    lag_2: float = 0.0
    lag_3: float = 0.0
    lag_4: float = 0.0
    lag_5: float = 0.0
    lag_6: float = 0.0
    rolling_mean_3: float = 0.0
    rolling_std_3: float = 0.0
    rolling_mean_6: float = 0.0
    rolling_max_3: float = 0.0
    history_month_count: float = 0.0
    month_num: float
    quarter: float
    year: float
    is_q4: float
    month_sin: float = 0.0
    month_cos: float = 0.0


class ForecastFeaturesRequest(BaseModel):
    rows: List[ForecastFeatureRow]


class ForecastFeaturesResponse(BaseModel):
    forecasts: List[CategoryForecast]
    model_name: str


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    load()
    yield


app = FastAPI(title="NeuralBudget M3 Forecast Service", version="2.0.0", lifespan=lifespan)

# Prometheus instrumentation
Instrumentator().instrument(app).expose(app)


# CORS + COEP — required for browser Worker fetches under require-corp
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
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    if _degraded:
        return {
            "status": "degraded",
            "model_version": None,
            "reason": "MLflow unreachable or model load failed",
        }
    return {"status": "ok", "model_name": _model_name, "model_version": _model_version}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M3 Forecast Service",
        "model": _model_name,
        "version": _model_version,
        "endpoints": ["/health", "/forecast/features", "/metrics", "/admin/reload"],
    }


@app.post("/forecast/features", response_model=ForecastFeaturesResponse)
def forecast_from_features(request: ForecastFeaturesRequest):
    if _degraded or _model is None:
        # Fail open — return empty list rather than error
        return ForecastFeaturesResponse(forecasts=[], model_name=_model_name)

    if not request.rows:
        return ForecastFeaturesResponse(forecasts=[], model_name=_model_name)

    rows_df = pd.DataFrame([row.model_dump() for row in request.rows])
    categories = rows_df["project_category"].tolist()

    X = pd.get_dummies(rows_df.drop(columns=["project_category"]), dummy_na=True)
    X = X.reindex(columns=_feature_cols, fill_value=0)

    preds = _model.predict(X)

    forecasts = [
        CategoryForecast(category=cat, forecast=max(0.0, float(pred)))
        for cat, pred in zip(categories, preds)
    ]
    # Sort descending by forecast amount
    forecasts.sort(key=lambda f: f.forecast or 0, reverse=True)

    M3_PREDICTIONS.labels(n_categories=str(len(forecasts))).inc()
    logger.info("Forecast for %d categories (model v%s)", len(forecasts), _model_version)

    # Derive the forecast_month from the first row's year/month_num fields (best effort).
    # Logged asynchronously so it never blocks the response.
    try:
        first_row = request.rows[0] if request.rows else None
        if first_row is not None:
            year = int(first_row.year)
            month = int(first_row.month_num)
            forecast_month = f"{year:04d}-{month:02d}"
            threading.Thread(
                target=log_forecasts,
                kwargs={
                    "forecast_month": forecast_month,
                    "category_forecasts": [(f.category, f.forecast or 0.0) for f in forecasts],
                    "model_version": _model_version,
                },
                daemon=True,
            ).start()
    except Exception as log_exc:
        logger.warning("Forecast log error (non-fatal): %s", log_exc)

    return ForecastFeaturesResponse(forecasts=forecasts, model_name=_model_name)


@app.post("/admin/reload")
def admin_reload(version: str | None = None):
    """Hot-reload model from MLflow. Mirrors M1's /admin/reload pattern."""
    if version:
        os.environ["M3_MODEL_VERSION"] = version
    elif "M3_MODEL_VERSION" in os.environ:
        del os.environ["M3_MODEL_VERSION"]
    threading.Thread(target=reload, daemon=True).start()
    return {"status": "reload_started", "current_version": _model_version, "pin_version": version}


@app.get("/metrics/forecast-accuracy")
def forecast_accuracy(
    version: str = Query(default=None, description="Model version to evaluate (default: current loaded version)"),
    actuals_url: str = Query(default=None, description="Override URL for fetching actuals (JSON: {month: {category: spend}})"),
):
    """Compute per-category MAE for a model version vs real actuals.

    Actuals are fetched from M3_ACTUALS_URL (env) or the actuals_url query param.
    The URL must return JSON in the shape:
        { "YYYY-MM": { "category_name": spend_float, ... }, ... }

    If no actuals URL is configured, returns the logged forecast records only
    (no MAE computed) — useful for the monitor daemon to verify forecasts are
    being recorded.
    """
    target_version = version or _model_version
    if target_version is None:
        return {"error": "no model version available", "mae_by_category": {}, "record_count": 0}

    from forecast_log import read_forecasts_for_version
    records = read_forecasts_for_version(target_version)
    record_count = len(records)

    # Try to fetch actuals
    url = actuals_url or ACTUALS_URL
    actuals: Dict[str, Dict[str, float]] = {}
    actuals_error = None
    if url:
        try:
            resp = http_requests.get(url, timeout=10)
            resp.raise_for_status()
            actuals = resp.json()
        except Exception as exc:
            actuals_error = str(exc)
            logger.warning("Could not fetch actuals from %s: %s", url, exc)

    mae_by_category: Dict[str, float] = {}
    if actuals:
        mae_by_category = compute_mae_vs_actuals(target_version, actuals)

    return {
        "model_version": target_version,
        "record_count": record_count,
        "categories_with_mae": len(mae_by_category),
        "mae_by_category": mae_by_category,
        "overall_mae": (
            float(np.mean(list(mae_by_category.values()))) if mae_by_category else None
        ),
        "actuals_source": url or None,
        "actuals_error": actuals_error,
    }
