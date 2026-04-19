import os
import uuid
from datetime import datetime
from typing import Optional

import joblib
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from db import get_conn

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
    "user_total_lag_1",
    "user_total_rolling_mean_3",
    "category_share_lag_1",
    "AGE_REF",
    "FAM_SIZE",
    "user_scale",
]
# FEATURE_COLUMNS = [
#     "monthly_spend",
#     "lag_1",
#     "lag_2",
#     "lag_3",
#     "lag_6",
#     "rolling_mean_3",
#     "rolling_std_3",
#     "rolling_mean_6",
#     "rolling_max_3",
#     "history_month_count",
#     "month_num",
#     "quarter",
#     "year",
#     "is_q4",
#     "month_sin",
#     "month_cos",
#     "budgeted",
# ]


def load_training_rows() -> pd.DataFrame:
    sql = """
          SELECT synthetic_user_id, \
                 category_name, \
                 year_month, \
                 monthly_spend, \
                 lag_1, \
                 lag_2, \
                 lag_3, \
                 lag_6, \
                 rolling_mean_3, \
                 rolling_std_3, \
                 rolling_mean_6, \
                 rolling_max_3, \
                 history_month_count, \
                 month_num, \
                 quarter, year, is_q4, month_sin, month_cos, budgeted, user_total_lag_1, user_total_rolling_mean_3, category_share_lag_1, age_ref AS "AGE_REF", fam_size AS "FAM_SIZE", user_scale, target_next_month
          FROM forecast_training_rows
          WHERE budget_id = 'csv-seeded-budget'
          ORDER BY year_month, category_name, synthetic_user_id \
          """

    # sql = """
    # SELECT
    #     category_name,
    #     year_month,
    #     monthly_spend,
    #     lag_1,
    #     lag_2,
    #     lag_3,
    #     lag_6,
    #     rolling_mean_3,
    #     rolling_std_3,
    #     rolling_mean_6,
    #     rolling_max_3,
    #     history_month_count,
    #     month_num,
    #     quarter,
    #     year,
    #     is_q4,
    #     month_sin,
    #     month_cos,
    #     budgeted,
    #     target_next_month
    # FROM forecast_training_rows
    # ORDER BY year_month, category_name
    # """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

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


def insert_retraining_run(
    run_id: str,
    status: str,
    train_rows: int,
    eval_rows: int,
    error_message: Optional[str] = None,
) -> None:
    sql = """
    INSERT INTO retraining_runs (
        run_id,
        status,
        train_rows,
        eval_rows,
        error_message
    )
    VALUES (%s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (run_id, status, train_rows, eval_rows, error_message),
            )


def insert_model_version(
    model_name: str,
    version: str,
    status: str,
    artifact_path: str,
    overall_mae: float,
    median_per_category_mae: Optional[float] = None,
    notes: Optional[str] = None,
) -> None:
    sql = """
    INSERT INTO model_versions (
        model_name,
        version,
        status,
        artifact_path,
        overall_mae,
        median_per_category_mae,
        notes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    model_name,
                    version,
                    status,
                    artifact_path,
                    overall_mae,
                    median_per_category_mae,
                    notes,
                ),
            )


def main() -> None:
    run_id = str(uuid.uuid4())

    df = load_training_rows()
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

    host_artifacts_dir = "training/m3/artifacts"
    container_artifacts_dir = os.getenv("M3_ARTIFACT_DIR", "/app/artifacts")

    os.makedirs(host_artifacts_dir, exist_ok=True)

    version = datetime.utcnow().strftime("m3_db_%Y%m%d_%H%M%S")

    host_artifact_path = os.path.join(host_artifacts_dir, f"{version}.joblib")
    container_artifact_path = os.path.join(
        container_artifacts_dir,
        f"{version}.joblib",
    )

    bundle = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "model_name": "m3-forecast-db",
        "version": version,
    }
    joblib.dump(bundle, host_artifact_path)

    print(f"Train rows: {len(train_df)}")
    print(f"Eval rows: {len(eval_df)}")
    print(f"MAE: {mae:.4f}")

    out = eval_df[["category_name", "year_month", "target_next_month"]].copy()
    out["prediction"] = preds
    print(out.to_string(index=False))

    insert_retraining_run(
        run_id=run_id,
        status="success",
        train_rows=len(train_df),
        eval_rows=len(eval_df),
    )

    insert_model_version(
        model_name="m3-forecast-db",
        version=version,
        status="candidate",
        artifact_path=container_artifact_path,
        overall_mae=float(mae),
        median_per_category_mae=None,
        notes="Retrained from PostgreSQL forecast_training_rows",
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        run_id = str(uuid.uuid4())
        insert_retraining_run(
            run_id=run_id,
            status="failed",
            train_rows=0,
            eval_rows=0,
            error_message=str(e),
        )
        raise