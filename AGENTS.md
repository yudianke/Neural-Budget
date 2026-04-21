# AGENTS.md — NeuralBudget MLOps Project

## Team
- **Chetan Kodeboyina** — Training (M1, M2, M3)
- **Yudian Ke** — Serving
- **Adib Zandkarim** — Data Pipeline

## Project Overview
We are building a **Smart Finance Assistant** as an optional ML layer on top of **ActualBudget** (github.com/actualbudget/actual, ~15k stars), a self-hosted open-source personal finance app. The system is deployed on **Chameleon Cloud**. Target: 10–300 users per instance. ActualBudget remains fully functional without the ML features.

### Three ML Features
1. **M1 — Transaction Auto-Categorization** ✅ INTEGRATED: Predicts spending category from payee name + amount. Auto-fills if confidence ≥ 0.6; shows top-3 suggestions if < 0.6.
2. **M2 — Anomaly Detection** 🔄 INTEGRATED: Flags duplicates, subscription price jumps, statistical outliers via warning badge. Inactive until user has ≥ 50 transactions. Isolation Forest + deterministic rules.
3. **M3 — Budget Forecasting** ⏳ INTEGRATED: Predicts next-month spending per category. Prophet for discretionary; deterministic for recurring bills. Falls back to MoneyData priors if < 12 months history.

---

## Current Integration Status

### M1 — PARTIAL COMPLETE ✅
M1 is fully wired end-to-end: training → FastAPI serving → loot-core client → React UI.

**What works:**
- Create a transaction with a known payee + non-zero amount → M1 predicts, auto-fills category if confidence ≥ 0.6
- M1 service down → transaction save is unaffected (errors swallowed to null)
- Fresh repo clone → migration `1776000000000_add_m1_categories.js` seeds all 13 M1-compatible categories automatically
- snake_case ↔ Title Case label resolution works (`personal_care` → `Personal Care`)

**Key files changed for M1:**
- `serving/m1_baseline/server.py` — added CORS middleware
- `serving/m1_baseline/real_model.py` — fixed feature skew, MLflow fallback, label encoder rebuild
- `serving/m1_baseline/schemas.py` — added optional fields with defaults
- `packages/loot-core/src/server/transactions/ml-service.ts` — new HTTP client for M1
- `packages/loot-core/src/server/transactions/transaction-rules.ts` — M1 called after user rules
- `packages/loot-core/migrations/1776000000000_add_m1_categories.js` — seeds 13 categories
- `actual/docker-compose.yml` — M1_SERVICE_URL, host.docker.internal, node_modules volumes

**Known M1 limitations (do not fix during M2 work):**
- `historical_majority_category_for_payee` is in the schema but not a trained feature — cold-start payees fall back to `misc`
- Top-3 suggestion UI not yet implemented
- `yarn typecheck` / `yarn lint:fix` not yet run on loot-core changes
- Oddity: older Utility transactions appeared to flip category after adding a new Dominion Power transaction — not reproduced, suspected browser cache artifact

### M2 — INTEGRATION STARTING NOW 🔄
The trained M2 model artifact exists on MLflow. Serving and UI wiring have not been done yet.

**MLflow artifact details:**
- MLflow UI: `http://129.114.26.214:8000/#/experiments`
- Artifact path: `mlflow-artifacts:/2/<Run ID>/artifacts/model.pkl`
- Direct download: `http://129.114.26.214:8000/ajax-api/2.0/mlflow/logged-models/m-c0a5e85dd6b0494ba3b1fa394db99480/artifacts/files?artifact_file_path=model.pkl`

### M3 — NOT STARTED ⏳

---

## Repository Structure
```
E:.
Neural-Budget
├── AGENTS.md
├── README.md
├── data_pipeline
│   ├── batch_pipeline.py
│   ├── generate_synthetic.py
│   ├── generator.py
│   ├── ingest_data.py
│   ├── manifest.json
│   └── online_features.py
├── serving
│   ├── benchmark
│   │   └── benchmark_requests.py
│   ├── m1_baseline
│   │   ├── Dockerfile
│   │   ├── README.md
│   │   ├── inspect_model.py
│   │   ├── mock_model.py
│   │   ├── real_model.py
│   │   ├── requirements.txt
│   │   ├── schemas.py
│   │   ├── server.py
│   │   └── test_load.py
│   ├── m1_onnx
│   │   ├── Dockerfile
│   │   ├── README.md
│   │   ├── app.py
│   │   ├── export_to_onnx.py
│   │   ├── model.onnx
│   │   └── requirements.txt
│   ├── m1_onnx_multiworker
│   │   ├── Dockerfile
│   │   ├── app.py
│   │   ├── gunicorn.conf.py
│   │   ├── model.onnx
│   │   └── requirements.txt
│   ├── m1_rayserve_bonus
│   │   ├── Dockerfile
│   │   ├── model.onnx
│   │   ├── requirements.txt
│   │   ├── run_serve.py
│   │   └── serve_app.py
│   └── samples
│       ├── m1_input.json
│       ├── m1_output.json
│       ├── m2_input.json
│       ├── m2_output.json
│       ├── m3_input.json
│       └── m3_output.json
└── training
    ├── _common.py
    ├── m1
    │   ├── Dockerfile.m1
    │   ├── config_baseline.yaml
    │   ├── config_logreg.yaml
    │   ├── config_m1.yaml
    │   ├── config_xgb_v2.yaml
    │   ├── config_xgb_v3.yaml
    │   ├── config_xgb_v4.yaml
    │   ├── requirements.txt
    │   └── train_m1.py
    ├── m1_ray
    │   ├── Dockerfile.m1_ray
    │   ├── config_m1_ray.yaml
    │   ├── ft_demo_output.log
    │   ├── requirements_ray.txt
    │   ├── train_m1_ray.py
    │   └── train_m1_ray_ft_demo.py
    ├── m2
    │   ├── Dockerfile.m2
    │   ├── config_m2.yaml
    │   ├── config_m2_v2.yaml
    │   ├── config_m2_v3.yaml
    │   ├── requirements_m2.txt
    │   └── train_m2.py
    └── m3
        ├── Dockerfile.m3
        ├── config_m3.yaml
        ├── config_m3_v2.yaml
        ├── config_m3_v3.yaml
        ├── requirements_m3.txt
        └── train_m3.py
```

---

## Claude Code Task: M2 Integration (Read This Section Carefully)

**Your job is to plan ONLY — do NOT write any code yet.**

### Step 1: Read these files in order
1. `AGENTS.md` (this file) — project context and constraints
2. `training/m2/train_m2.py` — understand what the model is, what features it expects, what it outputs
3. `training/m2/config_m2*.yaml` — understand hyperparameters and training config
4. `serving/m1_baseline/` — read ALL files  as your serving template
5. `serving/m1_onnx_multiworker/` — read ALL files as the preferred production pattern to follow for M2
6. `data_pipeline/generate_synthetic.py` — understand how synthetic data was created (already done, do not regenerate)
7. `serving/samples/` — find the M2 sample input/output JSON files

### Step 2: Scan ActualBudget UI to identify M2 integration points
Look in `actual/packages/desktop-client/src/components/`:
- `transactions/` — where the transaction table lives (M1 auto-fill happened here)
- `budget/` — budget page (M3 will go here, ignore for now)
- Identify: where should anomaly warning badges appear? What React component renders each transaction row?

Also look at:
- `packages/loot-core/src/server/transactions/transaction-rules.ts` — where M1 was hooked in; M2 will hook in nearby
- `packages/loot-core/src/server/transactions/ml-service.ts` — M1 HTTP client; M2 needs its own equivalent

### Step 3: Answer these questions before planning anything
1. What exact features does `train_m2.py` use? List them with types.
2. Does the trained `model.pkl` contain the Isolation Forest only, or also the scaler/preprocessor?
3. What does M2 output — a float score, a boolean, both? What is the threshold?
4. Does M2 need to be called synchronously (blocking transaction save) or async (badge shown after)? Per spec: **async, < 5s, badge shown after sync**.
5. Which transaction row React component in `desktop-client` would host the anomaly badge?
6. What are the deterministic rules (duplicate within 24h, subscription jump) — are these implemented in `train_m2.py` or will they need to be implemented separately in the serving layer?

### Step 4: Propose the minimal plan

Give me a numbered plan with these sections:

**A. Serving layer** (new folder and files `serving/m2_onnx_multiworker/`)
- How to download the newest model.pkl from MLflow (Now the m1_baseline only download one model not auto always download and use the best one, you can learn from it to learn the pattern)
- What port M2 runs on (suggest 8002 to avoid collision with M1 on 8001)
- What the `/predict` endpoint looks like (input schema → output schema)
- How to handle the deterministic rules (duplicate / subscription jump) — in the serving layer or in loot-core?
- Whether ONNX conversion makes sense for Isolation Forest (it does — sklearn-onnx supports it)

**B. loot-core changes**
- New file: `ml-service-m2.ts` (mirror of `ml-service.ts` for M1)
- Where in `transaction-rules.ts` to call M2 (after M1, async, non-blocking)
- How to pass the computed features (amount_zscore, frequency_ratio, rolling stats, repeat_count, is_recurring_candidate) — which of these are already on the transaction object vs. need to be computed fresh
- Whether rolling stats need a DB query and how expensive that is

**C. UI changes**
- Which component gets the anomaly badge
- What the badge looks like (warning icon, dismiss button)
- Where the user confirmation/dismissal gets logged as feedback

**D. Docker / docker-compose changes**
- New service for M2 at port 8002
- M2_SERVICE_URL env var in Actual container

**E. Migration (if needed)**
- Does M2 require any new DB columns (e.g., `anomaly_score`, `anomaly_dismissed`) on the transactions table?

**F. What to skip for now**
- Anything that is explicitly out of scope for the local end-to-end demo (Chameleon deployment, retraining pipeline, monitoring)

---

## Model Details

### M1 — TF-IDF + XGBoost (Categorization) ✅
- **Preprocessing**: Payee names normalized (strip txn IDs, resolve aliases e.g. AMZN → Amazon)
- **Features**: TF-IDF character n-grams (n=3–5, 500 features), log-amount, day-of-week, day-of-month, account type, historical majority category for payee
- **Training**: Chronological split; candidate promotes only when macro-F1 improves with < 2% regression on top-20 categories, else auto-rollback
- **Retraining**: Weekly, or immediately upon ≥ 50 corrections
- **Output**: Predicted category + softmax confidence + top-3 suggestions; auto-fill if ≥ 0.6
- **Serving port**: 8001

### M2 — Isolation Forest + Rules (Anomaly Detection) 🔄
- **Cold-start guard**: Inactive until user has ≥ 50 transactions
- **Rules**: Exact duplicates (same merchant + amount within 24h), subscription jumps (≥ 2× usual charge)
- **Features**: amount z-score (vs per-category baseline), frequency ratio vs weekly avg, rolling 30d spend mean/std, M1 confidence, transaction type, repeat count, is_recurring_candidate
- **Bootstrapping**: MoneyData 7-year spending history for initial per-category distributions
- **Retraining**: Weekly threshold recalibration from dismissal feedback, targeting ~1–3 alerts/week/user
- **Serving port**: 8002 (proposed)
- **Calling pattern**: ASYNC — M1 fills category first (sync), M2 scores anomaly after (async, badge shown < 5s)

### M3 — Prophet (Forecasting) ⏳
- **Serving port**: 8003 (proposed, TBD)

---

## JSON Interface Contracts

### M1 Input / Output (reference — already implemented)
```json
// Input
{
  "transaction_id": "txn_000000001",
  "synthetic_user_id": "user_1",
  "date": "2024-01-01",
  "merchant": "SUBWAY 26377 CORNE",
  "amount": -1.32,
  "transaction_type": "DEB",
  "account_type": "checking",
  "day_of_week": 0,
  "day_of_month": 1,
  "month": 1,
  "log_abs_amount": 0.8415671856782186,
  "historical_majority_category_for_payee": "restaurants"
}

// Output
{
  "transaction_id": "txn_000000001",
  "predicted_category": "restaurants",
  "confidence": 0.91,
  "top_3_suggestions": [
    {"category": "restaurants", "confidence": 0.91},
    {"category": "groceries", "confidence": 0.06},
    {"category": "misc", "confidence": 0.03}
  ],
  "auto_fill": true,
  "model_version": "m1_xgboost_v1"
}
```

### M2 Input / Output (target — to be served)
```json
// Input
{
  "transaction_id": "txn_000000001",
  "synthetic_user_id": "user_1",
  "date": "2024-01-01",
  "merchant": "SUBWAY 26377 CORNE",
  "project_category": "restaurants",
  "transaction_type": "DEB",
  "amount": -1.32,
  "abs_amount": 1.32,
  "m1_confidence": 0.91,
  "amount_zscore": -0.84,
  "frequency_ratio_vs_weekly_avg": 1.12,
  "rolling_30d_spend_mean": 18.40,
  "rolling_30d_spend_std": 9.75,
  "repeat_count": 1,
  "is_recurring_candidate": 0
}

// Output
{
  "transaction_id": "txn_000000001",
  "synthetic_user_id": "user_1",
  "anomaly_score": 0.18,
  "is_anomaly": false,
  "threshold": 0.72,
  "rule_flags": {
    "duplicate_within_24h": false,
    "subscription_jump": false,
    "amount_spike": false
  },
  "badge_type": null,
  "model_version": "m2_isolation_forest_v1"
}
```

### M3 Input / Output (reference only, not current task)
```json
// Input
{
  "synthetic_user_id": "user_1",
  "forecast_month": "2024-02",
  "month_of_year": 2,
  "holiday_indicator": 0,
  "monthly_category_spend_history": {
    "groceries": [142.3, 151.7, 136.1],
    "restaurants": [31.0, 28.4, 25.6],
    "housing": [47.0, 47.0, 47.0],
    "utilities": [53.6, 54.1, 52.9]
  },
  "recurring_bill_component": {
    "housing": 47.0,
    "utilities": 53.0
  }
}

// Output
{
  "synthetic_user_id": "user_1",
  "forecast_month": "2024-02",
  "predictions": {
    "groceries": {"point_forecast": 145.0, "confidence_interval": [132.0, 158.0]},
    "restaurants": {"point_forecast": 28.0, "confidence_interval": [20.0, 36.0]},
    "housing": {"point_forecast": 47.0, "confidence_interval": [47.0, 47.0]},
    "utilities": {"point_forecast": 54.0, "confidence_interval": [50.0, 58.0]}
  },
  "model_version": "m3_prophet_v1"
}
```

---

## Serving Requirements

| Component        | Latency Target       | Mode         | Port  | Notes                              |
|------------------|---------------------|--------------|-------|------------------------------------|
| Categorization   | < 0.5s per txn      | Synchronous  | 8001  | Inline with import ✅              |
| Anomaly scoring  | < 5s                | Asynchronous | 8002  | Badge shown after sync 🔄          |
| Forecasting      | Background job      | Batch        | 8003  | Hourly/daily refresh ⏳            |
| Typical load     | 1–5 req/s           |              |       | Normal usage                       |
| Peak load        | ~50 req/s           |              |       | Batch bank-sync events             |

- 2–3 Chameleon VM replicas behind load balancer, CPU-only
- Isolation Forest scoring sub-millisecond on 6-feature vector

### Serving Variants (M1 reference — mirror pattern for M2)
- `m1_baseline/` — FastAPI, CPU, simplest reference ← start here for understanding
- `m1_onnx/` — ONNX-optimized model serving
- `m1_onnx_multiworker/` — Gunicorn multi-worker ONNX ← preferred production pattern

---

## ActualBudget Integration Points

### Key Files/Directories
- `actual/packages/loot-core/` — Core business logic, DB, server, rules, transactions, budget
- `actual/packages/desktop-client/src/components/` — React UI components
  - `budget/` — Budget page components (where M3 forecasts will surface)
  - `transactions/` — Transaction table (where M1 is integrated; M2 badge goes here)
  - `accounts/` — Account views
- `actual/packages/sync-server/` — Sync server (Node.js/Express)
- `actual/packages/api/` — API layer

### M1 Integration Files (use as exact template for M2)
- `packages/loot-core/src/server/transactions/ml-service.ts` — HTTP client pattern
- `packages/loot-core/src/server/transactions/transaction-rules.ts` — hook point
- `packages/loot-core/migrations/1776000000000_add_m1_categories.js` — migration pattern
- `actual/docker-compose.yml` — how M1_SERVICE_URL and host.docker.internal are configured

### ActualBudget Tech Stack
- Frontend: React + TypeScript
- Backend: Node.js, SQLite (local-first), Express sync server
- Build: Vite, Yarn workspaces
- Desktop: Electron
- Database migrations in `loot-core/migrations/`


---

## External Datasets

### MoneyData (Firat et al., EUROVIS 2023)
- First publicly available real-world anonymized retail bank transaction dataset
- 7 years (July 2015–2022), 6,500+ transactions, single UK retail bank customer
- Fields: date, transaction_type, description, debit_amount, credit_amount, balance
- 20 manually verified spending categories
- **Usage**: Primary training data for M1; bootstraps M2 normal distributions; cold-start priors for M3

### BLS Consumer Expenditure Survey (CES/FMLI)
- **Usage**: Cross-user diversity simulation, cold-start traffic, anomaly threshold calibration, heterogeneous forecasting

### Synthetic Data (already generated — do not regenerate)
- MoneyData split into 7 yearly windows → `user_1` through `user_7`
- CES/FMLI household priors perturb category mix, amount distributions, recurring cadence
- All preprocessing versioned in MLflow

---

## Dataset Schemas

### synthetic_transactions.csv (M2 features sourced from here)
```
synthetic_user_id, source_household_id, persona_cluster, date, merchant, project_category,
transaction_type, amount, is_synthetic, transaction_id, abs_amount, day_of_week, day_of_month,
month, log_abs_amount, abs_amount_rounded, repeat_count, is_recurring_candidate
```

### Project Categories (20 classes)
charity, education, entertainment, gas, groceries, healthcare, housing, misc, personal_care, restaurants, shopping, transport, utilities, cash_transfers, and others from MoneyData mapping.

---

## Deployment & Infrastructure

### Chameleon Cloud
- All runs executed on Chameleon inside containers
- MLflow: `http://129.114.26.214:8000`
- Object storage for large datasets and model checkpoints

### Docker Containers
- `Dockerfile.m1` — M1 training
- `Dockerfile.m2` — M2 training
- `Dockerfile.m3` — M3 training
- `serving/m1_baseline/Dockerfile` — reference for M2 serving Dockerfile
- `serving/m1_onnx_multiworker/Dockerfile` — preferred production pattern

---

## Safeguarding Plan
See **[SAFEGUARDING.md](./SAFEGUARDING.md)** for the full system-wide safeguarding plan covering all six dimensions (fairness, explainability, transparency, privacy, accountability, robustness) across M1, M2, and M3.

**Summary of implemented mechanisms:**
- **Fairness:** Per-category F1 gate (M1), dismiss-rate contamination adjustment (M2), per-category MAE regression gate (M3)
- **Explainability:** Top-50 TF-IDF n-gram artifact in MLflow (M1), badge type + rule flags in UI (M2), per-category forecast breakdown (M3)
- **Transparency:** Confidence score surfaced to user (M1), degraded-mode disclosure (M2), Grafana dashboards for all models
- **Privacy:** PII card-number scan on training data (M1), minimal feature payload to serving (M2), no demographic features (M3)
- **Accountability:** Git SHA + data MD5 logged per run (M1, M3), dismiss JSONL + MLflow retrain events (M2)
- **Robustness:** Low-confidence rate metric + quality gates (M1), cold-start guard + timeout + contamination floor (M2), three-gate promotion + automated MAE rollback (M3)
