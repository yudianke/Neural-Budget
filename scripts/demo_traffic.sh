#!/usr/bin/env bash
# =============================================================================
# demo_traffic.sh — Production Data Simulation for NeuralBudget Demo Video
# =============================================================================
# Sends realistic transactions to M1/M2/M3 endpoints, emulating how
# ActualBudget integrates with the ML layer in production.
#
# Usage:
#   chmod +x scripts/demo_traffic.sh
#   ./scripts/demo_traffic.sh
#   # Or with a remote server:
#   BASE_URL=http://129.114.27.248 ./scripts/demo_traffic.sh
# =============================================================================
set -euo pipefail

BASE="${BASE_URL:-http://localhost}"
M1="$BASE:8001"
M2="$BASE:8003"
M3="$BASE:8002"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

section() { echo -e "\n${CYAN}========== $1 ==========${NC}\n"; }
label()   { echo -e "${YELLOW}>>> $1${NC}"; }

# ─── Health Checks ───────────────────────────────────────────────────────────
section "HEALTH CHECKS"

label "M1 Serving (Categorization)"
curl -s "$M1/health" | python3 -m json.tool

label "M2 Serving (Anomaly Detection)"
curl -s "$M2/health" | python3 -m json.tool

label "M3 Serving (Forecasting)"
curl -s "$M3/health" | python3 -m json.tool

# ─── M1: Transaction Categorization ─────────────────────────────────────────
section "M1 — TRANSACTION CATEGORIZATION"

label "1) Grocery store (expect high confidence, auto_fill=true)"
curl -s -X POST "$M1/predict/category" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-001",
    "synthetic_user_id": "demo-user",
    "date": "2026-04-19",
    "merchant": "TESCO SUPERMARKET",
    "amount": -45.23,
    "transaction_type": "debit",
    "account_type": "checking",
    "day_of_week": 5,
    "day_of_month": 19,
    "month": 4,
    "log_abs_amount": 3.81,
    "historical_majority_category_for_payee": "groceries"
  }' | python3 -m json.tool

label "2) Restaurant chain"
curl -s -X POST "$M1/predict/category" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-002",
    "synthetic_user_id": "demo-user",
    "date": "2026-04-18",
    "merchant": "MCDONALDS",
    "amount": -12.50,
    "transaction_type": "debit",
    "account_type": "checking",
    "day_of_week": 4,
    "day_of_month": 18,
    "month": 4,
    "log_abs_amount": 2.53,
    "historical_majority_category_for_payee": "restaurants"
  }' | python3 -m json.tool

label "3) Ambiguous merchant (expect lower confidence, suggestions)"
curl -s -X POST "$M1/predict/category" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-003",
    "synthetic_user_id": "demo-user",
    "date": "2026-04-17",
    "merchant": "AMAZON MARKETPLACE",
    "amount": -29.99,
    "transaction_type": "debit",
    "account_type": "checking",
    "day_of_week": 3,
    "day_of_month": 17,
    "month": 4,
    "log_abs_amount": 3.40,
    "historical_majority_category_for_payee": ""
  }' | python3 -m json.tool

label "4) Gas station"
curl -s -X POST "$M1/predict/category" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-004",
    "synthetic_user_id": "demo-user",
    "date": "2026-04-16",
    "merchant": "SHELL OIL STATION",
    "amount": -55.00,
    "transaction_type": "debit",
    "account_type": "checking",
    "day_of_week": 2,
    "day_of_month": 16,
    "month": 4,
    "log_abs_amount": 4.01,
    "historical_majority_category_for_payee": "gas"
  }' | python3 -m json.tool

label "5) Utility bill"
curl -s -X POST "$M1/predict/category" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-005",
    "synthetic_user_id": "demo-user",
    "date": "2026-04-01",
    "merchant": "ELECTRIC COMPANY PAYMENT",
    "amount": -120.00,
    "transaction_type": "debit",
    "account_type": "checking",
    "day_of_week": 2,
    "day_of_month": 1,
    "month": 4,
    "log_abs_amount": 4.79,
    "historical_majority_category_for_payee": "utilities"
  }' | python3 -m json.tool

# ─── M2: Anomaly Detection ──────────────────────────────────────────────────
section "M2 — ANOMALY DETECTION"

label "6) Normal transaction (should NOT be flagged)"
curl -s -X POST "$M2/predict/anomaly" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-006",
    "synthetic_user_id": "demo-user",
    "abs_amount": 45.23,
    "repeat_count": 5,
    "is_recurring_candidate": 1,
    "user_txn_index": 120,
    "user_mean_abs_amount_prior": 52.00,
    "user_std_abs_amount_prior": 30.00,
    "duplicate_within_24h": false,
    "subscription_jump": false,
    "merchant": "TESCO",
    "date": "2026-04-19"
  }' | python3 -m json.tool

label "7) Spending spike (10x normal — expect badge_type=spike)"
curl -s -X POST "$M2/predict/anomaly" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-007",
    "synthetic_user_id": "demo-user",
    "abs_amount": 520.00,
    "repeat_count": 0,
    "is_recurring_candidate": 0,
    "user_txn_index": 121,
    "user_mean_abs_amount_prior": 52.00,
    "user_std_abs_amount_prior": 30.00,
    "duplicate_within_24h": false,
    "subscription_jump": false,
    "merchant": "LUXURY STORE",
    "date": "2026-04-19"
  }' | python3 -m json.tool

label "8) Duplicate within 24h (expect badge_type=duplicate)"
curl -s -X POST "$M2/predict/anomaly" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-008",
    "synthetic_user_id": "demo-user",
    "abs_amount": 45.23,
    "repeat_count": 2,
    "is_recurring_candidate": 0,
    "user_txn_index": 122,
    "user_mean_abs_amount_prior": 52.00,
    "user_std_abs_amount_prior": 30.00,
    "duplicate_within_24h": true,
    "subscription_jump": false,
    "merchant": "TESCO",
    "date": "2026-04-19"
  }' | python3 -m json.tool

label "9) Subscription price jump (expect badge_type=price_jump)"
curl -s -X POST "$M2/predict/anomaly" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "demo-009",
    "synthetic_user_id": "demo-user",
    "abs_amount": 29.99,
    "repeat_count": 12,
    "is_recurring_candidate": 1,
    "user_txn_index": 123,
    "user_mean_abs_amount_prior": 14.99,
    "user_std_abs_amount_prior": 0.50,
    "duplicate_within_24h": false,
    "subscription_jump": true,
    "merchant": "NETFLIX",
    "date": "2026-04-19"
  }' | python3 -m json.tool

# ─── M3: Budget Forecasting ─────────────────────────────────────────────────
section "M3 — BUDGET FORECASTING"

label "10) Forecast next-month spend for 3 categories"
curl -s -X POST "$M3/forecast/features" \
  -H "Content-Type: application/json" \
  -d '{
    "rows": [
      {
        "project_category": "groceries",
        "monthly_spend": 450,
        "lag_1": 450, "lag_2": 420, "lag_3": 480, "lag_4": 400, "lag_5": 460, "lag_6": 430,
        "rolling_mean_3": 450, "rolling_std_3": 30, "rolling_mean_6": 440, "rolling_max_3": 480,
        "history_month_count": 24,
        "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0,
        "month_sin": 0.866, "month_cos": 0.5
      },
      {
        "project_category": "restaurants",
        "monthly_spend": 180,
        "lag_1": 180, "lag_2": 200, "lag_3": 150, "lag_4": 190, "lag_5": 170, "lag_6": 160,
        "rolling_mean_3": 176, "rolling_std_3": 25, "rolling_mean_6": 175, "rolling_max_3": 200,
        "history_month_count": 24,
        "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0,
        "month_sin": 0.866, "month_cos": 0.5
      },
      {
        "project_category": "utilities",
        "monthly_spend": 120,
        "lag_1": 120, "lag_2": 115, "lag_3": 130, "lag_4": 110, "lag_5": 125, "lag_6": 118,
        "rolling_mean_3": 121, "rolling_std_3": 7.6, "rolling_mean_6": 119, "rolling_max_3": 130,
        "history_month_count": 24,
        "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0,
        "month_sin": 0.866, "month_cos": 0.5
      },
      {
        "project_category": "housing",
        "monthly_spend": 1500,
        "lag_1": 1500, "lag_2": 1500, "lag_3": 1500, "lag_4": 1500, "lag_5": 1500, "lag_6": 1500,
        "rolling_mean_3": 1500, "rolling_std_3": 0, "rolling_mean_6": 1500, "rolling_max_3": 1500,
        "history_month_count": 24,
        "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0,
        "month_sin": 0.866, "month_cos": 0.5
      },
      {
        "project_category": "transport",
        "monthly_spend": 200,
        "lag_1": 200, "lag_2": 180, "lag_3": 220, "lag_4": 195, "lag_5": 210, "lag_6": 190,
        "rolling_mean_3": 200, "rolling_std_3": 20, "rolling_mean_6": 199, "rolling_max_3": 220,
        "history_month_count": 24,
        "month_num": 5, "quarter": 2, "year": 2026, "is_q4": 0,
        "month_sin": 0.866, "month_cos": 0.5
      }
    ]
  }' | python3 -m json.tool

# ─── Feedback Examples ───────────────────────────────────────────────────────
section "FEEDBACK — Closing the Loop"

label "11) M1 feedback: user corrects a wrong prediction"
curl -s -X POST "$M1/feedback" \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {
        "transaction_id": "demo-003",
        "date": "2026-04-17",
        "amount": -29.99,
        "merchant": "AMAZON MARKETPLACE",
        "predicted_category": "shopping",
        "chosen_category": "entertainment",
        "confidence": 0.55,
        "feedback_type": "overridden"
      }
    ]
  }' | python3 -m json.tool

label "12) M1 feedback: user accepts correct prediction"
curl -s -X POST "$M1/feedback" \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {
        "transaction_id": "demo-001",
        "date": "2026-04-19",
        "amount": -45.23,
        "merchant": "TESCO SUPERMARKET",
        "predicted_category": "groceries",
        "chosen_category": "groceries",
        "confidence": 0.92,
        "feedback_type": "accepted"
      }
    ]
  }' | python3 -m json.tool

label "13) M2 feedback: user dismisses false positive spike"
curl -s -X POST "$M2/feedback" \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {
        "transaction_id": "demo-007",
        "feedback_type": "dismiss_false_positive",
        "badge_type": "spike",
        "anomaly_score": -0.25,
        "rule_flags": {"duplicate_within_24h": false, "subscription_jump": false, "amount_spike": true},
        "merchant": "LUXURY STORE",
        "amount": -520.00,
        "date": "2026-04-19"
      }
    ]
  }' | python3 -m json.tool

label "14) M2 feedback: user confirms duplicate is real"
curl -s -X POST "$M2/feedback" \
  -H "Content-Type: application/json" \
  -d '{
    "entries": [
      {
        "transaction_id": "demo-008",
        "feedback_type": "confirmed_anomaly",
        "badge_type": "duplicate",
        "anomaly_score": -0.40,
        "rule_flags": {"duplicate_within_24h": true, "subscription_jump": false, "amount_spike": false},
        "merchant": "TESCO",
        "amount": -45.23,
        "date": "2026-04-19"
      }
    ]
  }' | python3 -m json.tool

# ─── Feedback Stats ──────────────────────────────────────────────────────────
section "FEEDBACK STATS (used by retrain daemons)"

label "15) M1 feedback stats"
curl -s "$M1/metrics/feedback" | python3 -m json.tool

label "16) M2 feedback stats"
curl -s "$M2/metrics/feedback" | python3 -m json.tool

# ─── Prometheus Metrics (sample) ─────────────────────────────────────────────
section "PROMETHEUS METRICS (sample)"

label "17) M1 custom metrics"
curl -s "$M1/metrics" 2>/dev/null | grep -E "^m1_" | head -10

label "18) M2 custom metrics"
curl -s "$M2/metrics" 2>/dev/null | grep -E "^m2_" | head -10

label "19) M3 custom metrics"
curl -s "$M3/metrics" 2>/dev/null | grep -E "^m3_" | head -10

# ─── Done ────────────────────────────────────────────────────────────────────
section "DONE"
echo -e "${GREEN}All production data endpoints exercised successfully!${NC}"
echo ""
echo "Next steps for demo:"
echo "  - Open ActualBudget UI:  $BASE:3001"
echo "  - Open Grafana:          $BASE:3000  (admin / neuralbudget)"
echo "  - Open Prometheus:       $BASE:9090"
echo "  - Open MLflow:           http://129.114.26.214:8000"
