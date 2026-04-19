import pandas as pd

from db import get_conn


INPUT_PATH = "training/m3/monthly_category_history_sample.csv"


def load_history(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    required = [
        "budget_id",
        "category_id",
        "category_name",
        "year_month",
        "monthly_spend",
        "budgeted",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df[required].copy()


def upsert_monthly_history(df: pd.DataFrame) -> None:
    sql = """
    INSERT INTO monthly_category_history (
        budget_id,
        category_id,
        category_name,
        year_month,
        monthly_spend,
        budgeted
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (budget_id, category_id, year_month)
    DO UPDATE SET
        category_name = EXCLUDED.category_name,
        monthly_spend = EXCLUDED.monthly_spend,
        budgeted = EXCLUDED.budgeted;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                cur.execute(
                    sql,
                    (
                        row.budget_id,
                        row.category_id,
                        row.category_name,
                        row.year_month,
                        float(row.monthly_spend),
                        float(row.budgeted),
                    ),
                )


def main() -> None:
    df = load_history(INPUT_PATH)
    upsert_monthly_history(df)
    print(f"Upserted {len(df)} rows into monthly_category_history")


if __name__ == "__main__":
    main()