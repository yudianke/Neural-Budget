"""
NeuralBudget Continuous Load Generator
=======================================
Runs indefinitely inside Docker, simulating realistic user activity against
M1 (categorization), M2 (anomaly detection), and M3 (forecasting) endpoints.

Designed to satisfy the April 29 – May 3 "emulated load running continuously"
course requirement without SSH intervention.

Behaviour
---------
Every CYCLE_SECONDS (default 30) it:
  1. Picks a random transaction from a realistic merchant/category pool
  2. POST /predict/category  → M1
  3. POST /predict/anomaly   → M2
  4. Waits FEEDBACK_DELAY_SECONDS (default 8)
  5. POST /feedback          → M1  (80% accepted, 20% overridden)
  6. Every 10th cycle: POST /forecast/features → M3

Feedback is intentionally biased: ~20% overrides are on deliberately wrong
predictions to ensure the correction JSONL accumulates real signal that the
retrain daemon can consume.

Environment variables (all optional):
  M1_URL              http://m1-serving:8001
  M2_URL              http://m2-serving:8003
  M3_URL              http://m3-serving:8002
  CYCLE_SECONDS       30
  FEEDBACK_DELAY_SECONDS  8
  LOG_LEVEL           INFO
  RANDOM_SEED         (unset = non-deterministic)
"""

import json
import logging
import math
import os
import random
import sys
import time
from datetime import date, timedelta

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
M1_URL = os.environ.get("M1_URL", "http://m1-serving:8001")
M2_URL = os.environ.get("M2_URL", "http://m2-serving:8003")
M3_URL = os.environ.get("M3_URL", "http://m3-serving:8002")
CYCLE_SECONDS = float(os.environ.get("CYCLE_SECONDS", "30"))
FEEDBACK_DELAY = float(os.environ.get("FEEDBACK_DELAY_SECONDS", "8"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

_seed = os.environ.get("RANDOM_SEED")
if _seed is not None:
    random.seed(int(_seed))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[LOAD %(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("load_generator")

# ---------------------------------------------------------------------------
# Realistic transaction pool
# Each entry: (merchant, true_category, typical_amount_range, account_type)
# ---------------------------------------------------------------------------
TRANSACTION_POOL = [
    # Groceries
    ("TESCO SUPERSTORE",         "groceries",     (20,  95),  "checking"),
    ("WHOLE FOODS MARKET",       "groceries",     (30, 120),  "checking"),
    ("TRADER JOES",              "groceries",     (15,  80),  "checking"),
    ("WALMART GROCERY",          "groceries",     (25, 110),  "checking"),
    ("ALDI STORES",              "groceries",     (10,  60),  "checking"),
    ("SAINSBURYS",               "groceries",     (20,  90),  "checking"),
    # Restaurants
    ("MCDONALDS",                "restaurants",   (5,   25),  "checking"),
    ("SUBWAY 26377",             "restaurants",   (6,   18),  "checking"),
    ("STARBUCKS",                "restaurants",   (4,   12),  "checking"),
    ("DOMINOS PIZZA",            "restaurants",   (12,  40),  "checking"),
    ("CHIPOTLE MEXICAN GRILL",   "restaurants",   (8,   20),  "checking"),
    ("PRET A MANGER",            "restaurants",   (5,   15),  "checking"),
    # Shopping
    ("AMAZON MARKETPLACE",       "shopping",      (10, 150),  "checking"),
    ("EBAY PURCHASE",            "shopping",      (5,  100),  "checking"),
    ("H&M FASHION",              "shopping",      (15,  80),  "checking"),
    ("ZARA ONLINE",              "shopping",      (20, 120),  "checking"),
    # Utilities
    ("BRITISH GAS ENERGY",       "utilities",     (50, 180),  "checking"),
    ("THAMES WATER",             "utilities",     (30,  80),  "checking"),
    ("ELECTRIC COMPANY PMT",     "utilities",     (60, 200),  "checking"),
    ("VIRGIN MEDIA BROADBAND",   "utilities",     (40,  80),  "checking"),
    # Transport
    ("TFL TRAVEL",               "transport",     (3,   25),  "checking"),
    ("SHELL OIL STATION",        "gas",           (30,  90),  "checking"),
    ("BP FUEL",                  "gas",           (25,  85),  "checking"),
    ("UBER TRIP",                "transport",     (8,   40),  "checking"),
    ("TRAINLINE TICKETS",        "transport",     (15, 120),  "checking"),
    # Entertainment
    ("NETFLIX SUBSCRIPTION",     "entertainment", (10,  20),  "checking"),
    ("SPOTIFY PREMIUM",          "entertainment", (10,  15),  "checking"),
    ("ODEON CINEMA",             "entertainment", (8,   30),  "checking"),
    ("STEAM GAMES",              "entertainment", (5,   60),  "checking"),
    # Healthcare
    ("BOOTS PHARMACY",           "healthcare",    (5,   50),  "checking"),
    ("NHS PRESCRIPTION",         "healthcare",    (9,   30),  "checking"),
    # Housing
    ("LANDLORD RENT PMT",        "housing",       (800, 1800), "checking"),
    ("COUNCIL TAX",              "housing",       (80, 250),  "checking"),
    # Personal care
    ("SUPERDRUG",                "personal_care", (5,   40),  "checking"),
    ("SALON SERVICES",           "personal_care", (20,  80),  "checking"),
    # Education
    ("UDEMY COURSE",             "education",     (10,  50),  "checking"),
    ("AMAZON BOOKS",             "education",     (8,   30),  "checking"),
    # Misc / ambiguous (intentionally harder for M1)
    ("PAYPAL PAYMENT",           "misc",          (5,  200),  "checking"),
    ("GOOGLE PLAY",              "entertainment", (1,   15),  "checking"),
    ("APPLE ITUNES",             "entertainment", (1,   10),  "checking"),
]

# Wrong categories intentionally used for override feedback (simulates user corrections)
WRONG_CATEGORY_MAP = {
    "groceries":     "shopping",
    "restaurants":   "entertainment",
    "shopping":      "misc",
    "utilities":     "housing",
    "gas":           "transport",
    "transport":     "misc",
    "entertainment": "shopping",
    "healthcare":    "misc",
    "housing":       "misc",
    "personal_care": "misc",
    "education":     "shopping",
    "misc":          "shopping",
}

# M3 feature rows per category (realistic stable user)
M3_ROWS = [
    {"project_category": "groceries",    "monthly_spend": 460, "lag_1": 460, "lag_2": 430, "lag_3": 480, "lag_4": 410, "lag_5": 450, "lag_6": 440, "rolling_mean_3": 456, "rolling_std_3": 25, "rolling_mean_6": 445, "rolling_max_3": 480, "history_month_count": 24, "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0, "month_sin": 0.866, "month_cos": 0.5},
    {"project_category": "restaurants",  "monthly_spend": 175, "lag_1": 175, "lag_2": 190, "lag_3": 160, "lag_4": 180, "lag_5": 170, "lag_6": 155, "rolling_mean_3": 175, "rolling_std_3": 15, "rolling_mean_6": 171, "rolling_max_3": 190, "history_month_count": 24, "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0, "month_sin": 0.866, "month_cos": 0.5},
    {"project_category": "utilities",    "monthly_spend": 130, "lag_1": 130, "lag_2": 125, "lag_3": 140, "lag_4": 120, "lag_5": 135, "lag_6": 128, "rolling_mean_3": 131, "rolling_std_3": 7,  "rolling_mean_6": 129, "rolling_max_3": 140, "history_month_count": 24, "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0, "month_sin": 0.866, "month_cos": 0.5},
    {"project_category": "transport",    "monthly_spend": 90,  "lag_1": 90,  "lag_2": 85,  "lag_3": 100, "lag_4": 88,  "lag_5": 92,  "lag_6": 80,  "rolling_mean_3": 91,  "rolling_std_3": 7,  "rolling_mean_6": 89,  "rolling_max_3": 100, "history_month_count": 24, "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0, "month_sin": 0.866, "month_cos": 0.5},
    {"project_category": "shopping",     "monthly_spend": 200, "lag_1": 200, "lag_2": 180, "lag_3": 220, "lag_4": 190, "lag_5": 210, "lag_6": 170, "rolling_mean_3": 200, "rolling_std_3": 20, "rolling_mean_6": 195, "rolling_max_3": 220, "history_month_count": 24, "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0, "month_sin": 0.866, "month_cos": 0.5},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _today_offset(days: int = 0) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _make_txn_id(cycle: int, suffix: str = "") -> str:
    return f"loadgen-{int(time.time())}-c{cycle}{suffix}"


def _post(url: str, payload: dict, label: str) -> dict | None:
    try:
        r = requests.post(url, json=payload, timeout=8)
        if r.ok:
            log.debug(f"{label} → {r.status_code}")
            return r.json()
        log.warning(f"{label} returned {r.status_code}: {r.text[:120]}")
    except requests.exceptions.ConnectionError:
        log.warning(f"{label} — connection refused (service may be starting)")
    except requests.exceptions.Timeout:
        log.warning(f"{label} — timed out")
    except Exception as e:
        log.warning(f"{label} — unexpected error: {e}")
    return None


def _wait_for_services(max_wait: int = 120) -> None:
    """Block until all three services respond to /health, up to max_wait seconds."""
    deadline = time.monotonic() + max_wait
    checks = {
        "M1": f"{M1_URL}/health",
        "M2": f"{M2_URL}/health",
        "M3": f"{M3_URL}/health",
    }
    pending = set(checks.keys())
    log.info("Waiting for ML services to become healthy...")
    while pending and time.monotonic() < deadline:
        for name in list(pending):
            try:
                r = requests.get(checks[name], timeout=4)
                if r.ok and r.json().get("status") in ("ok", "degraded"):
                    log.info(f"  {name} ready ({r.json().get('status')})")
                    pending.discard(name)
            except Exception:
                pass
        if pending:
            time.sleep(5)
    if pending:
        log.warning(f"Services still not ready after {max_wait}s: {pending}. Continuing anyway.")


# ---------------------------------------------------------------------------
# Per-cycle work
# ---------------------------------------------------------------------------
def run_cycle(cycle: int) -> None:
    merchant, true_cat, amount_range, account_type = random.choice(TRANSACTION_POOL)
    amount = round(random.uniform(*amount_range), 2)
    txn_date = _today_offset(random.randint(0, 3))
    txn_id = _make_txn_id(cycle)
    d = date.fromisoformat(txn_date)

    log_abs = round(math.log1p(amount), 6)

    # ── M1: predict category ────────────────────────────────────────────────
    m1_payload = {
        "transaction_id": txn_id,
        "synthetic_user_id": "loadgen-user",
        "date": txn_date,
        "merchant": merchant,
        "amount": -amount,
        "transaction_type": "DEB",
        "account_type": account_type,
        "day_of_week": d.weekday(),
        "day_of_month": d.day,
        "month": d.month,
        "log_abs_amount": log_abs,
        "historical_majority_category_for_payee": true_cat,
    }
    m1_result = _post(f"{M1_URL}/predict/category", m1_payload, "M1 predict")

    predicted_cat = true_cat  # fallback if M1 is degraded
    confidence = 0.5
    top3 = []
    if m1_result:
        predicted_cat = m1_result.get("predicted_category", true_cat)
        confidence = m1_result.get("confidence", 0.5)
        top3 = m1_result.get("top_3_suggestions", [])
        log.info(
            f"[C{cycle:04d}] {merchant[:28]:<28} → {predicted_cat:<16} "
            f"conf={confidence:.2f} {'✓' if predicted_cat == true_cat else '✗'}"
        )

    # ── M2: anomaly score ───────────────────────────────────────────────────
    # Occasionally inject an artificial spike to exercise all badge types
    spike = (cycle % 47 == 0)          # every 47 cycles ~ once every ~24 min
    dup   = (cycle % 73 == 0)          # every 73 cycles
    jump  = (cycle % 101 == 0)         # every 101 cycles

    m2_payload = {
        "transaction_id": txn_id,
        "synthetic_user_id": "loadgen-user",
        "abs_amount": amount * (10 if spike else 1),
        "repeat_count": random.randint(0, 8),
        "is_recurring_candidate": 1 if random.random() < 0.3 else 0,
        "user_txn_index": 60 + cycle,
        "user_mean_abs_amount_prior": 55.0,
        "user_std_abs_amount_prior": 28.0,
        "duplicate_within_24h": dup,
        "subscription_jump": jump,
        "merchant": merchant,
        "date": txn_date,
        "m1_confidence": confidence,
    }
    m2_result = _post(f"{M2_URL}/predict/anomaly", m2_payload, "M2 anomaly")
    if m2_result and m2_result.get("is_anomaly"):
        log.info(f"[C{cycle:04d}] M2 anomaly flagged: badge={m2_result.get('badge_type')}")

    # ── Wait before feedback (simulates user review time) ───────────────────
    time.sleep(FEEDBACK_DELAY)

    # ── M1: feedback ────────────────────────────────────────────────────────
    # 20% of the time send an override with a wrong category so corrections
    # accumulate in the feedback JSONL for the retrain daemon.
    override = random.random() < 0.20
    chosen_cat = WRONG_CATEGORY_MAP.get(true_cat, "misc") if override else true_cat
    feedback_type = "overridden" if override else "accepted"

    feedback_payload = {
        "entries": [{
            "transaction_id": txn_id,
            "date": txn_date,
            "amount": -amount,
            "merchant": merchant,
            "imported_payee": merchant,
            "predicted_category": predicted_cat,
            "chosen_category": chosen_cat,
            "confidence": confidence,
            "feedback_type": feedback_type,
            "top_3_suggestions": top3,
            "source": "load_generator",
        }]
    }
    fb_result = _post(f"{M1_URL}/feedback", feedback_payload, "M1 feedback")
    if fb_result:
        log.debug(f"[C{cycle:04d}] feedback logged={fb_result.get('logged')} type={feedback_type}")

    # ── M3: forecast every 10th cycle ───────────────────────────────────────
    if cycle % 10 == 0:
        m3_payload = {"rows": M3_ROWS}
        m3_result = _post(f"{M3_URL}/forecast/features", m3_payload, "M3 forecast")
        if m3_result:
            preds = m3_result.get("predictions", {})
            cats_done = ", ".join(list(preds.keys())[:3])
            log.info(f"[C{cycle:04d}] M3 forecast OK — categories: {cats_done}...")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("NeuralBudget Load Generator starting")
    log.info(f"  M1={M1_URL}  M2={M2_URL}  M3={M3_URL}")
    log.info(f"  cycle={CYCLE_SECONDS}s  feedback_delay={FEEDBACK_DELAY}s")

    _wait_for_services()

    cycle = 0
    while True:
        cycle_start = time.monotonic()
        try:
            run_cycle(cycle)
        except Exception as e:
            log.exception(f"Unexpected error in cycle {cycle}: {e}")

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, CYCLE_SECONDS - elapsed)
        log.debug(f"Cycle {cycle} done in {elapsed:.1f}s, sleeping {sleep_for:.1f}s")
        time.sleep(sleep_for)
        cycle += 1


if __name__ == "__main__":
    main()
