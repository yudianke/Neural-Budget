"""
M3 Forecast Inference Service

Loads the latest registered m3-forecast-v2 bundle from MLflow at startup
(same pattern as M1's real_model.py). Exposes /forecast/features for the
ActualBudget backend worker to call.

Environment variables:
    MLFLOW_TRACKING_URI     MLflow server URL
    M3_REGISTERED_MODEL     MLflow model name (default: m3-forecast-v2)
    M3_MODEL_VERSION        Pin a specific version (optional, default: latest)

Endpoints:
    GET  /health                    Service health + loaded model version
    POST /forecast/features         Real-time forecast from feature rows
    GET  /metrics                   Prometheus metrics
"""
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import List, Optional

import joblib
import mlflow
import numpy as np
import pandas as pd
from fastapi import FastAPI
from mlflow.tracking import MlflowClient
from prometheus_client import Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("m3_service")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MLFLOW_URI   = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.27.211:8000")
MODEL_NAME   = os.environ.get("M3_REGISTERED_MODEL", "m3-forecast-v2")
MODEL_VER    = os.environ.get("M3_MODEL_VERSION")   # None = latest

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
    if MODEL_VER:
        return MODEL_VER
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
    user_total_lag_1: float = 0.0
    user_total_rolling_mean_3: float = 0.0
    category_share_lag_1: float = 0.0


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
