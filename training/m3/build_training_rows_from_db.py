import pandas as pd
import numpy as np
from db import get_conn


def load_monthly_history() -> pd.DataFrame:
    sql = """
    SELECT
        budget_id,
        category_id,
        category_name,
        year_month,
        monthly_spend,
        budgeted
    FROM monthly_category_history
    ORDER BY budget_id, category_id, year_month
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    dt = pd.to_datetime(df["year_month"] + "-01")
    df["month_num"] = dt.dt.month
    df["quarter"] = dt.dt.quarter
    df["year"] = dt.dt.year
    df["is_q4"] = (df["month_num"].isin([10, 11, 12])).astype(int)

    df["month_sin"] = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_num"] / 12)
    return df


def build_training_rows(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for (budget_id, category_id), g in df.groupby(["budget_id", "category_id"]):
        g = g.sort_values("year_month").reset_index(drop=True).copy()

        g["lag_1"] = g["monthly_spend"].shift(1)
        g["lag_2"] = g["monthly_spend"].shift(2)
        g["lag_3"] = g["monthly_spend"].shift(3)
        g["lag_6"] = g["monthly_spend"].shift(6)

        g["rolling_mean_3"] = g["monthly_spend"].shift(1).rolling(3).mean()
        g["rolling_std_3"] = g["monthly_spend"].shift(1).rolling(3).std()
        g["rolling_mean_6"] = g["monthly_spend"].shift(1).rolling(6).mean()
        g["rolling_max_3"] = g["monthly_spend"].shift(1).rolling(3).max()

        g["history_month_count"] = range(1, len(g) + 1)
        g["target_next_month"] = g["monthly_spend"].shift(-1)

        rows.append(g)

    out = pd.concat(rows, ignore_index=True)

    numeric_fill_zero = [
        "lag_1",
        "lag_2",
        "lag_3",
        "lag_6",
        "rolling_mean_3",
        "rolling_std_3",
        "rolling_mean_6",
        "rolling_max_3",
    ]
    out[numeric_fill_zero] = out[numeric_fill_zero].fillna(0)

    out = out.dropna(subset=["target_next_month"]).copy()

    return out


def upsert_training_rows(df: pd.DataFrame) -> None:
    delete_sql = "DELETE FROM forecast_training_rows;"

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
        target_next_month
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_sql)

            for row in df.itertuples(index=False):
                cur.execute(
                    insert_sql,
                    (
                        row.budget_id,
                        row.category_id,
                        row.category_name,
                        row.year_month,
                        float(row.monthly_spend),
                        float(row.lag_1),
                        float(row.lag_2),
                        float(row.lag_3),
                        float(row.lag_6),
                        float(row.rolling_mean_3),
                        float(row.rolling_std_3),
                        float(row.rolling_mean_6),
                        float(row.rolling_max_3),
                        int(row.history_month_count),
                        int(row.month_num),
                        int(row.quarter),
                        int(row.year),
                        int(row.is_q4),
                        float(row.month_sin),
                        float(row.month_cos),
                        float(row.budgeted),
                        float(row.target_next_month),
                    ),
                )


def main() -> None:
    df = load_monthly_history()
    df = add_time_features(df)
    rows = build_training_rows(df)
    upsert_training_rows(rows)
    print(f"Built and stored {len(rows)} forecast training rows")


if __name__ == "__main__":
    main()