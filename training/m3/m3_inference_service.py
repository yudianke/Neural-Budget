import logging
import os
from typing import List, Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("m3_service")

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


class ForecastFeaturesRequest(BaseModel):
    rows: List[ForecastFeatureRow]


class ForecastFeaturesResponse(BaseModel):
    forecasts: List[CategoryForecast]
    model_name: str

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


bundle = load_bundle(MODEL_BUNDLE_PATH)
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


@app.post("/forecast/user", response_model=UserForecastResponse)
def forecast_for_user(request: UserForecastRequest):
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

    # Apply same encoding as training
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

@app.post("/forecast/features", response_model=ForecastFeaturesResponse)
def forecast_from_features(request: ForecastFeaturesRequest):
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
    logger.info(f"Feature-based forecast for {len(forecasts)} categories")

    return ForecastFeaturesResponse(
        forecasts=forecasts,
        model_name=model_name,
    )
