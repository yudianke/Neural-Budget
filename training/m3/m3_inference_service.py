import logging
import os
from typing import List, Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, Gauge
from fastapi import Response
import time

from db import get_conn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("m3_service")


LOCAL_CANDIDATE_MAE = Gauge(
    "local_candidate_mae",
    "Latest local candidate MAE",
    ["user_id"],
)

GLOBAL_BASELINE_MAE = Gauge(
    "global_baseline_mae",
    "Global production MAE on local holdout",
    ["user_id"],
)

REQUEST_COUNT = Counter(
    "forecast_requests_total",
    "Total number of forecast requests"
)

MODEL_LOAD_SUCCESS = Counter(
    "model_load_success_total",
    "Number of successful model loads"
)

MODEL_LOAD_FAILURE = Counter(
    "model_load_failure_total",
    "Number of failed model loads"
)

CURRENT_MODEL_VERSION = Gauge(
    "current_model_version",
    "Current production model version (timestamp-based)"
)

MODEL_LAST_LOADED_TS = Gauge(
    "model_last_loaded_timestamp",
    "Timestamp when model was last loaded"
)

REQUEST_ERRORS = Counter(
    "forecast_errors_total",
    "Total number of forecast errors"
)

REQUEST_LATENCY = Histogram(
    "forecast_request_latency_seconds",
    "Latency of forecast requests"
)

MODEL_BUNDLE_PATH = os.getenv("M3_V2_BUNDLE_PATH", "m3_v2_bundle.joblib")
FEATURE_ROWS_PATH = os.getenv("M3_V2_FEATURES_PATH", "m3_v2_latest_features.csv")



class CategoryForecast(BaseModel):
    category: str
    forecast: Optional[float]


class UserForecastRequest(BaseModel):
    synthetic_user_id: str


class UserForecastResponse(BaseModel):
    synthetic_user_id: str
    forecasts: List[CategoryForecast]
    model_name: str

class ForecastFeatureRow(BaseModel):
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
    user_total_lag_1: float = 0.0
    user_total_rolling_mean_3: float = 0.0
    category_share_lag_1: float = 0.0

class TransactionIn(BaseModel):
    user_id: str
    transaction_id: str
    date: str
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    amount: float
    payee: Optional[str] = None

class ForecastFeaturesRequest(BaseModel):
    rows: List[ForecastFeatureRow]


class ForecastFeaturesResponse(BaseModel):
    forecasts: List[CategoryForecast]
    model_name: str


def resolve_model_bundle_path() -> str:
    fallback = os.getenv("M3_V2_BUNDLE_PATH", "m3_v2_bundle.joblib")

    sql = """
    SELECT artifact_path
    FROM model_versions
    WHERE status IN ('production', 'candidate')
    ORDER BY
        CASE WHEN status = 'production' THEN 0 ELSE 1 END,
        trained_at DESC
    LIMIT 1
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()

        if row and row[0]:
            path = row[0]
            logger.info("Resolved model bundle from DB registry: %s", path)
            return path

    except Exception as e:
        logger.warning("Falling back to env bundle path due to DB lookup error: %s", e)

    logger.info("Resolved model bundle from fallback path: %s", fallback)
    return fallback

def load_bundle(path: str):
    if not os.path.exists(path):
        raise RuntimeError(f"Model bundle not found: {path}")
    bundle = joblib.load(path)
    logger.info("Loaded model bundle from %s", path)
    return bundle


def load_feature_rows(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise RuntimeError(f"Feature rows file not found: {path}")
    df = pd.read_csv(path)
    logger.info("Loaded latest feature rows from %s (%d rows)", path, len(df))
    return df

def refresh_local_model_comparison_metrics() -> None:
    sql = """
    SELECT DISTINCT ON (user_id)
        user_id,
        local_candidate_mae,
        global_baseline_mae
    FROM local_model_comparisons
    ORDER BY user_id, created_at DESC
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()

        for user_id, local_mae, global_mae in rows:
            LOCAL_CANDIDATE_MAE.labels(user_id=user_id).set(float(local_mae))
            GLOBAL_BASELINE_MAE.labels(user_id=user_id).set(float(global_mae))
    except Exception as e:
        logger.warning("Failed to refresh local/global MAE metrics: %s", e)

bundle = load_bundle(resolve_model_bundle_path())
model = bundle["model"]
feature_columns = bundle["feature_columns"]
model_name = bundle.get("model_name", "m3-forecast-v2")

feature_df = load_feature_rows(FEATURE_ROWS_PATH)

app = FastAPI(title="M3 Forecast Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_loaded": True,
        "model_name": model_name,
    }

@app.post("/transactions/ingest")
def ingest_transaction(tx: TransactionIn):
    sql = """
    INSERT INTO local_user_transactions (
        user_id,
        transaction_id,
        date,
        category_id,
        category_name,
        amount,
        payee
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, transaction_id)
    DO UPDATE SET
        date = EXCLUDED.date,
        category_id = EXCLUDED.category_id,
        category_name = EXCLUDED.category_name,
        amount = EXCLUDED.amount,
        payee = EXCLUDED.payee
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tx.user_id,
                    tx.transaction_id,
                    tx.date,
                    tx.category_id,
                    tx.category_name,
                    tx.amount,
                    tx.payee,
                ),
            )

    return {"status": "ok"}
@app.post("/forecast/user", response_model=UserForecastResponse)
def forecast_for_user(request: UserForecastRequest):
    REQUEST_COUNT.inc()
    start = time.time()

    try:
        user_id = request.synthetic_user_id

        user_rows = feature_df[feature_df["synthetic_user_id"] == user_id].copy()
        if user_rows.empty:
            raise HTTPException(status_code=404, detail=f"User not found: {user_id}")

        categories = user_rows["project_category"].tolist()

        drop_cols = [
            "synthetic_user_id",
            "project_category",
            "year_month",
            "target_next_month_spend",
        ]
        X = user_rows.drop(columns=[c for c in drop_cols if c in user_rows.columns])

        X = pd.get_dummies(X, dummy_na=True)
        X = X.reindex(columns=feature_columns, fill_value=0)

        preds = model.predict(X)

        forecasts = [
            CategoryForecast(category=cat, forecast=float(pred))
            for cat, pred in sorted(
                zip(categories, preds),
                key=lambda x: x[1],
                reverse=True,
            )
        ]

        logger.info("Forecasted %d categories for user %s", len(forecasts), user_id)

        return UserForecastResponse(
            synthetic_user_id=user_id,
            forecasts=forecasts,
            model_name=model_name,
        )

    except Exception:
        REQUEST_ERRORS.inc()
        raise

    finally:
        REQUEST_LATENCY.observe(time.time() - start)
@app.get("/metrics")
def metrics():
    refresh_local_model_comparison_metrics()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
@app.post("/forecast/features", response_model=ForecastFeaturesResponse)
def forecast_from_features(request: ForecastFeaturesRequest):
    REQUEST_COUNT.inc()
    start = time.time()

    try:
        if not request.rows:
            raise HTTPException(status_code=400, detail="No feature rows provided")

        rows_df = pd.DataFrame([row.model_dump() for row in request.rows])

        categories = rows_df["project_category"].tolist()

        X = rows_df.copy()
        X = pd.get_dummies(X, dummy_na=True)
        X = X.reindex(columns=feature_columns, fill_value=0)

        preds = model.predict(X)

        forecasts = [
            CategoryForecast(category=cat, forecast=float(pred))
            for cat, pred in sorted(
                zip(categories, preds),
                key=lambda x: x[1],
                reverse=True,
            )
        ]
        logger.info("Feature-based forecast for %d categories", len(forecasts))

        return ForecastFeaturesResponse(
            forecasts=forecasts,
            model_name=model_name,
        )

    except Exception:
        REQUEST_ERRORS.inc()
        raise

    finally:
        REQUEST_LATENCY.observe(time.time() - start)
