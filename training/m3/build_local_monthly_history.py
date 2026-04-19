import pandas as pd

from db import get_conn


def load_local_transactions() -> pd.DataFrame:
    sql = """
    SELECT
        user_id,
        transaction_id,
        date,
        category_id,
        category_name,
        amount
    FROM local_user_transactions
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn)

    return df


def build_monthly_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "user_id",
                "category_id",
                "category_name",
                "year_month",
                "monthly_spend",
                "budgeted",
            ]
        )

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["year_month"] = out["date"].dt.strftime("%Y-%m")

    # For spending forecasts, treat expenses as positive spend
    out["spend_amount"] = out["amount"].apply(lambda x: abs(float(x)) if float(x) < 0 else 0.0)

    grouped = (
        out.groupby(["user_id", "category_id", "category_name", "year_month"], dropna=False)["spend_amount"]
        .sum()
        .reset_index()
        .rename(columns={"spend_amount": "monthly_spend"})
    )

    grouped["budgeted"] = 0.0
    return grouped


def replace_local_monthly_history(df: pd.DataFrame) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM local_monthly_category_history;")

            insert_sql = """
            INSERT INTO local_monthly_category_history (
                user_id,
                category_id,
                category_name,
                year_month,
                monthly_spend,
                budgeted
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            rows = [
                (
                    row.user_id,
                    row.category_id,
                    row.category_name,
                    row.year_month,
                    float(row.monthly_spend),
                    float(row.budgeted),
                )
                for row in df.itertuples(index=False)
            ]

            if rows:
                cur.executemany(insert_sql, rows)


def main() -> None:
    tx_df = load_local_transactions()
    monthly_df = build_monthly_history(tx_df)
    replace_local_monthly_history(monthly_df)
    print(f"Built {len(monthly_df)} local monthly history rows")


if __name__ == "__main__":
    main()