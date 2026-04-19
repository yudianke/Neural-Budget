import math

import pandas as pd

from db import get_conn


TRAIN_PATH = "data_pipeline/processed/batch_datasets/forecasting_v2_train.csv"
EVAL_PATH = "data_pipeline/processed/batch_datasets/forecasting_v2_eval.csv"


EXPECTED_COLUMNS = [
    "synthetic_user_id",
    "project_category",
    "year_month",
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
    "user_total_lag_1",
    "user_total_rolling_mean_3",
    "category_share_lag_1",
    "persona_cluster",
    "AGE_REF",
    "FAM_SIZE",
    "user_scale",
    "target_next_month_spend",
]


def safe_float(x) -> float:
    return float(x) if pd.notna(x) else 0.0


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")

    return df[EXPECTED_COLUMNS].copy()


def normalize_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["budget_id"] = "csv-seeded-budget"
    out["category_id"] = out["project_category"]
    out["category_name"] = out["project_category"]
    out["budgeted"] = 0.0
    out["target_next_month"] = out["target_next_month_spend"]

    return out


def replace_training_rows(df: pd.DataFrame) -> None:
    delete_sql = """
    DELETE FROM forecast_training_rows
    WHERE budget_id = 'csv-seeded-budget';
    """

    insert_sql = """
    INSERT INTO forecast_training_rows (
        budget_id,
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
        target_next_month,
        synthetic_user_id,
        user_total_lag_1,
        user_total_rolling_mean_3,
        category_share_lag_1,
        persona_cluster,
        age_ref,
        fam_size,
        user_scale
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_sql)

            rows_to_insert = []

            for row in df.itertuples(index=False):
                month_num = float(row.month_num)
                month_sin = math.sin(2 * math.pi * month_num / 12)
                month_cos = math.cos(2 * math.pi * month_num / 12)

                rows_to_insert.append(
                    (
                        row.budget_id,
                        row.category_id,
                        row.category_name,
                        row.year_month,
                        safe_float(row.monthly_spend),
                        safe_float(row.lag_1),
                        safe_float(row.lag_2),
                        safe_float(row.lag_3),
                        safe_float(row.lag_6),
                        safe_float(row.rolling_mean_3),
                        safe_float(row.rolling_std_3),
                        safe_float(row.rolling_mean_6),
                        safe_float(row.rolling_max_3),
                        int(row.history_month_count),
                        int(row.month_num),
                        int(row.quarter),
                        int(row.year),
                        int(row.is_q4),
                        float(month_sin),
                        float(month_cos),
                        safe_float(row.budgeted),
                        safe_float(row.target_next_month),
                        str(row.synthetic_user_id),
                        safe_float(row.user_total_lag_1),
                        safe_float(row.user_total_rolling_mean_3),
                        safe_float(row.category_share_lag_1),
                        str(row.persona_cluster),
                        safe_float(row.AGE_REF),
                        safe_float(row.FAM_SIZE),
                        safe_float(row.user_scale),
                    )
                )

            cur.executemany(insert_sql, rows_to_insert)


def main() -> None:
    train_df = normalize_rows(load_csv(TRAIN_PATH))
    eval_df = normalize_rows(load_csv(EVAL_PATH))

    df = pd.concat([train_df, eval_df], ignore_index=True)
    replace_training_rows(df)

    print(f"Inserted {len(df)} rows into forecast_training_rows for csv-seeded-budget")


if __name__ == "__main__":
    main()