import os
from datetime import datetime
from typing import Optional
import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from db import get_conn
from local_model_eligibility import has_sufficient_local_history


FEATURE_COLUMNS = [
    "monthly_spend",
    "lag_1",
    "lag_2",
    "lag_3",
    "lag_6",
    "rolling_mean_3",
    "rolling_std_3",
    "rolling_mean_6",
    "rolling_max_3",
    "history_month_count",
    "month_num",
    "quarter",
    "year",
    "is_q4",
    "month_sin",
    "month_cos",
    "budgeted",
]


def load_local_training_rows(user_id: str) -> pd.DataFrame:
    sql = """
    SELECT
        user_id,
        category_id,
        category_name,
        year_month,
        monthly_spend,
        lag_1,
        lag_2,
        lag_3,
        lag_6,
        rolling_mean_3,
        rolling_std_3,
        rolling_mean_6,
        rolling_max_3,
        history_month_count,
        month_num,
        quarter,
        year,
        is_q4,
        month_sin,
        month_cos,
        budgeted,
        target_next_month
    FROM local_forecast_training_rows
    WHERE user_id = %s
    ORDER BY year_month, category_name
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=[user_id])

    return df


def split_train_eval(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    months = sorted(df["year_month"].unique().tolist())
    if len(months) < 2:
        raise ValueError("Need at least 2 distinct months for train/eval split")

    train_months = months[:-1]
    eval_month = months[-1]

    train_df = df[df["year_month"].isin(train_months)].copy()
    eval_df = df[df["year_month"] == eval_month].copy()

    if train_df.empty or eval_df.empty:
        raise ValueError("Train or eval split is empty")

    return train_df, eval_df


def insert_local_model_version(
    user_id: str,
    model_name: str,
    version: str,
    status: str,
    artifact_path: str,
    overall_mae: float,
    notes: Optional[str] = None,
) -> None:
    sql = """
    INSERT INTO local_user_model_versions (
        user_id,
        model_name,
        version,
        status,
        artifact_path,
        overall_mae,
        notes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    user_id,
                    model_name,
                    version,
                    status,
                    artifact_path,
                    overall_mae,
                    notes,
                ),
            )


def main(user_id: str) -> None:
    if not has_sufficient_local_history(user_id):
        print(f"User {user_id} is not eligible for local training yet")
        return

    df = load_local_training_rows(user_id)
    train_df, eval_df = split_train_eval(df)

    X_train = train_df[FEATURE_COLUMNS].copy()
    y_train = train_df["target_next_month"].copy()

    X_eval = eval_df[FEATURE_COLUMNS].copy()
    y_eval = eval_df["target_next_month"].copy()

    model = HistGradientBoostingRegressor(
        max_depth=4,
        learning_rate=0.05,
        max_iter=200,
        random_state=42,
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_eval)
    mae = mean_absolute_error(y_eval, preds)

    host_dir = os.path.join("training", "m3", "artifacts", "users", user_id)
    container_dir = os.path.join("/app", "artifacts", "users", user_id)
    os.makedirs(host_dir, exist_ok=True)

    version = datetime.utcnow().strftime(f"local_{user_id}_%Y%m%d_%H%M%S")
    host_artifact_path = os.path.join(host_dir, f"{version}.joblib")
    container_artifact_path = os.path.join(container_dir, f"{version}.joblib")

    bundle = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "model_name": "m3-local-forecast",
        "version": version,
        "user_id": user_id,
    }
    joblib.dump(bundle, host_artifact_path)

    insert_local_model_version(
        user_id=user_id,
        model_name="m3-local-forecast",
        version=version,
        status="candidate",
        artifact_path=container_artifact_path,
        overall_mae=float(mae),
        notes="Local personalized model trained from local_forecast_training_rows",
    )

    print(f"User: {user_id}")
    print(f"Train rows: {len(train_df)}")
    print(f"Eval rows: {len(eval_df)}")
    print(f"MAE: {mae:.4f}")
    print(f"Saved candidate artifact: {host_artifact_path}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python training/m3/train_local_user_model.py <user_id>")

    main(sys.argv[1])