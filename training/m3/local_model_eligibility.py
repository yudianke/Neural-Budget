from db import get_conn


MIN_MONTHS = 4
MIN_CATEGORIES = 2
MIN_ROWS = 5


def has_sufficient_local_history(user_id: str) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    COUNT(DISTINCT year_month),
                    COUNT(DISTINCT category_name)
                FROM local_monthly_category_history
                WHERE user_id = %s
            """, (user_id,))
            months, categories = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*)
                FROM local_forecast_training_rows
                WHERE user_id = %s
            """, (user_id,))
            rows = cur.fetchone()[0]

    print(f"[Eligibility] user={user_id} months={months}, categories={categories}, rows={rows}")

    return (
        months >= MIN_MONTHS and
        categories >= MIN_CATEGORIES and
        rows >= MIN_ROWS
    )