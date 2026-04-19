import os
from contextlib import contextmanager

import psycopg2


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://nb_user:nb_pass@localhost:5433/neuralbudget",
)


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def test_connection() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            _ = cur.fetchone()