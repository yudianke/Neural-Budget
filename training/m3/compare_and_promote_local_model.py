import os

import joblib
import pandas as pd
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
]

PROMOTION_THRESHOLD = 0.95


def load_local_eval_rows(user_id: str) -> pd.DataFrame:
    sql = """
    SELECT
        user_id,
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

    if df.empty:
        raise ValueError(f"No local training rows found for user {user_id}")

    latest_month = sorted(df["year_month"].unique().tolist())[-1]
    return df[df["year_month"] == latest_month].copy()


def load_latest_local_candidate(user_id: str) -> tuple[str, str]:
    sql = """
    SELECT version, artifact_path
    FROM local_user_model_versions
    WHERE user_id = %s AND status = 'candidate'
    ORDER BY trained_at DESC
    LIMIT 1
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            row = cur.fetchone()

    if not row:
        raise ValueError(f"No candidate local model found for user {user_id}")

    return row[0], row[1]


def load_global_production_model() -> tuple[str, str]:
    sql = """
    SELECT version, artifact_path
    FROM model_versions
    WHERE status = 'production'
    ORDER BY trained_at DESC
    LIMIT 1
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

    if not row:
        raise ValueError("No global production model found")

    return row[0], row[1]


def container_to_host_path(path: str) -> str:
    if path.startswith("/app/artifacts/"):
        suffix = path[len("/app/artifacts/"):]
        return os.path.join("training", "m3", "artifacts", suffix)
    return path


def load_bundle(path: str) -> dict:
    host_path = container_to_host_path(path)
    if not os.path.exists(host_path):
        raise FileNotFoundError(f"Artifact not found on host: {host_path}")
    return joblib.load(host_path)


def archive_existing_local_production(user_id: str) -> None:
    sql = """
    UPDATE local_user_model_versions
    SET status = 'archived'
    WHERE user_id = %s AND status = 'production'
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))


def promote_local_candidate(user_id: str, version: str) -> None:
    archive_existing_local_production(user_id)

    sql = """
    UPDATE local_user_model_versions
    SET status = 'production',
        promoted_at = NOW()
    WHERE user_id = %s AND version = %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, version))


def archive_local_candidate(user_id: str, version: str) -> None:
    sql = """
    UPDATE local_user_model_versions
    SET status = 'archived'
    WHERE user_id = %s AND version = %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, version))


def main(user_id: str) -> None:
    eval_df = load_local_eval_rows(user_id)
    y_eval = eval_df["target_next_month"].copy()

    local_version, local_artifact = load_latest_local_candidate(user_id)
    global_version, global_artifact = load_global_production_model()

    local_bundle = load_bundle(local_artifact)
    global_bundle = load_bundle(global_artifact)

    local_model = local_bundle["model"]
    global_model = global_bundle["model"]

    local_feature_columns = local_bundle["feature_columns"]
    global_feature_columns = global_bundle["feature_columns"]

    X_local = eval_df.reindex(columns=local_feature_columns, fill_value=0).copy()
    X_global = eval_df.reindex(columns=global_feature_columns, fill_value=0).copy()

    local_preds = local_model.predict(X_local)
    global_preds = global_model.predict(X_global)

    local_mae = mean_absolute_error(y_eval, local_preds)
    global_mae = mean_absolute_error(y_eval, global_preds)
    if local_mae < global_mae * PROMOTION_THRESHOLD:
        promote_local_candidate(user_id, local_version)
        decision = "promoted"
        print("decision: promoted local candidate to production")
    else:
        archive_local_candidate(user_id, local_version)
        decision = "kept_global"
        print("decision: kept global baseline; archived local candidate")

    insert_local_model_comparison(
        user_id=user_id,
        local_version=local_version,
        global_version=global_version,
        local_mae=float(local_mae),
        global_mae=float(global_mae),
        decision=decision,
    )
    print(f"user_id: {user_id}")
    print(f"local_candidate_version: {local_version}")
    print(f"global_production_version: {global_version}")
    print(f"local_candidate_mae: {local_mae:.4f}")
    print(f"global_production_mae: {global_mae:.4f}")


def insert_local_model_comparison(
    user_id: str,
    local_version: str,
    global_version: str,
    local_mae: float,
    global_mae: float,
    decision: str,
) -> None:
    sql = """
    INSERT INTO local_model_comparisons (
        user_id,
        local_version,
        global_version,
        local_candidate_mae,
        global_baseline_mae,
        decision
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    user_id,
                    local_version,
                    global_version,
                    local_mae,
                    global_mae,
                    decision,
                ),
            )

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit(
            "Usage: python training/m3/compare_and_promote_local_model.py <user_id>"
        )

    main(sys.argv[1])