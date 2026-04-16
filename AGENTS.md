# AGENTS.md — NeuralBudget MLOps Project

## Team
- **Chetan Kodeboyina** — Training (M1, M2, M3)
- **Yudian Ke** — Serving
- **Adib Zandkarim** — Data Pipeline

## Project Overview
We are building a **Smart Finance Assistant** as an optional ML layer on top of **ActualBudget** (github.com/actualbudget/actual, ~15k stars), a self-hosted open-source personal finance app. The system is deployed on **Chameleon Cloud**. Target: 10–300 users per instance. ActualBudget remains fully functional without the ML features.

### Three ML Features
1. **M1 — Transaction Auto-Categorization**: Predicts spending category from payee name + amount. Auto-fills if confidence ≥ 0.6; shows top-3 suggestions if < 0.6.
2. **M2 — Anomaly Detection**: Flags duplicates, subscription price jumps, statistical outliers via warning badge. Inactive until user has ≥ 50 transactions. Isolation Forest + deterministic rules.
3. **M3 — Budget Forecasting**: Predicts next-month spending per category. Prophet for discretionary; deterministic for recurring bills. Falls back to MoneyData priors if < 12 months history.

---

## Repository Structure
```
E:.
├── actual/                    # ActualBudget open-source app (TypeScript/React)
├── datasets/                  # Raw + synthetic data files
│   ├── ces_household_category_spend.csv
│   ├── fmli241x.csv           # BLS Consumer Expenditure Survey (FMLI)
│   ├── moneydata.csv           # Raw MoneyData (EUROVIS 2023)
│   ├── moneydata_labeled.csv   # Labeled MoneyData with project categories
│   ├── mtbi241x.csv            # BLS CES expenditure items
│   ├── synthetic_transactions.csv
│   └── synthetic_users.csv
├── data_pipeline/
│   └── generate_synthetic.py
├── serving/
│   ├── benchmark/benchmark_requests.py
│   ├── m1_baseline/           # FastAPI baseline (CPU)
│   ├── m1_onnx/               # ONNX-optimized serving
│   ├── m1_onnx_multiworker/   # Gunicorn multi-worker ONNX
│   ├── m1_rayserve_bonus/     # Ray Serve bonus implementation
│   └── samples/               # JSON input/output samples for M1, M2, M3
├── training/
│   ├── m1/                    # XGBoost categorization training
│   │   ├── train_m1.py
│   │   ├── config_*.yaml      # Multiple config files (baseline, logreg, xgb variants)
│   │   ├── Dockerfile.m1
│   │   └── data/moneydata.csv
│   ├── m1_ray/                # Ray Train integration (bonus)
│   ├── m2/                    # Isolation Forest anomaly detection
│   │   ├── train_m2.py
│   │   ├── config_m2*.yaml
│   │   └── Dockerfile.m2
│   └── m3/                    # Prophet forecasting
│       ├── train_m3.py
│       ├── config_m3*.yaml
│       └── Dockerfile.m3
└── upcoming-release-notes/
```

---

## External Datasets

### MoneyData (Firat et al., EUROVIS 2023)
- First publicly available real-world anonymized retail bank transaction dataset
- 7 years (July 2015–2022), 6,500+ transactions, single UK retail bank customer
- Fields: date, transaction_type, description, debit_amount, credit_amount, balance
- 20 manually verified spending categories
- Peer-reviewed, University of Nottingham
- **Usage**: Primary training data for M1; bootstraps M2 normal distributions; cold-start priors for M3

### BLS Consumer Expenditure Survey (CES/FMLI)
- `fmli241x.csv`: Household-level demographic and financial data (income, family size, region, spending categories)
- `mtbi241x.csv`: Individual expenditure items (UCC codes, costs, reference months)
- **Usage**: Cross-user diversity simulation, cold-start traffic, anomaly threshold calibration, heterogeneous forecasting

### Synthetic Data Strategy
- MoneyData split into 7 yearly windows → `user_1` through `user_7` (~938 txns each, 12 months)
- CES/FMLI household priors perturb category mix, amount distributions, recurring cadence
- All preprocessing logic, partition versions, perturbation configs, dataset snapshot hashes logged in MLflow

---

## Model Details

### M1 — TF-IDF + XGBoost (Categorization)
- **Preprocessing**: Payee names normalized (strip txn IDs, resolve aliases e.g. AMZN → Amazon)
- **Features**: TF-IDF character n-grams (n=3–5, 500 features), log-amount, day-of-week, day-of-month, account type, historical majority category for payee
- **Training**: Chronological split; candidate promotes only when macro-F1 improves with < 2% regression on top-20 categories, else auto-rollback
- **Retraining**: Weekly, or immediately upon ≥ 50 corrections
- **Output**: Predicted category + softmax confidence + top-3 suggestions; auto-fill if ≥ 0.6

### M2 — Isolation Forest + Rules (Anomaly Detection)
- **Cold-start guard**: Inactive until user has ≥ 50 transactions
- **Rules**: Exact duplicates (same merchant + amount within 24h), subscription jumps (≥ 2× usual charge)
- **Features**: amount z-score (vs per-category baseline), frequency ratio vs weekly avg, rolling 30d spend mean/std, M1 confidence, transaction type, repeat count, is_recurring_candidate
- **Bootstrapping**: MoneyData 7-year spending history for initial per-category distributions
- **Retraining**: Weekly threshold recalibration from dismissal feedback, targeting ~1–3 alerts/week/user

### M3 — Prophet (Forecasting)
- **Separation**: Recurring bills (rent, subscriptions) forecast deterministically; Prophet handles discretionary only
- **Features**: Monthly category spend (12–18 months), month-of-year, holiday indicator, recurring-bill component
- **Cold-start**: Users with < 12 months history fall back to MoneyData-derived per-category monthly averages
- **Retraining**: Monthly; month M trained exclusively on data prior to M
- **Output**: Point forecast + confidence interval per category

---

## JSON Interface Contracts

### M1 Input
```json
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
```

### M1 Output
```json
{
  "transaction_id": "txn_000000001",
  "synthetic_user_id": "user_1",
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

### M2 Input
```json
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
```

### M2 Output
```json
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

### M3 Input
```json
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
```

### M3 Output
```json
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

| Component        | Latency Target       | Mode         | Notes                              |
|------------------|---------------------|--------------|------------------------------------|
| Categorization   | < 0.5s per txn      | Synchronous  | Inline with import                 |
| Anomaly scoring  | < 5s                | Asynchronous | Badge shown after sync             |
| Forecasting      | Background job      | Batch        | Hourly/daily refresh               |
| Typical load     | 1–5 req/s           |              | Normal usage                       |
| Peak load        | ~50 req/s           |              | Batch bank-sync events             |

- 2–3 Chameleon VM replicas behind load balancer, CPU-only
- TF-IDF vectorization + XGBoost inference < 10ms per txn on CPU
- Isolation Forest scoring sub-millisecond on 6-feature vector

### Serving Variants Implemented
- `m1_baseline/` — FastAPI, CPU, simplest reference
- `m1_onnx/` — ONNX-optimized model serving
- `m1_onnx_multiworker/` — Gunicorn multi-worker ONNX
- `m1_rayserve_bonus/` — Ray Serve (bonus)

---

## Data Pipeline Design

### Ingestion Flow
1. Transactions arrive via bank sync or CSV upload (timestamp, amount, payee, account)
2. Payee strings lowercased, stripped of transaction IDs → TF-IDF vectorization for M1
3. Amount, frequency, rolling statistics → 6-feature numeric vector for M2
4. Feature construction uses only historical data prior to each transaction
5. Rolling per-payee and per-category statistics updated asynchronously
6. Aggregated monthly category totals for M3

### Leakage Prevention
- Training examples include only confirmed labels (accepted suggestions or user corrections)
- All dataset splits strictly chronological
- Aggregate features computed on training data only; frozen statistics applied to validation/test
- M3 uses only data from prior months
- Feedback events excluded from the training set of the model version that generated them

### Feedback & Continuous Retraining Loop
- Every user action (category corrections, anomaly confirmations, budget edits) → immutable feedback log versioned alongside model artifacts in MLflow
- M1: Weekly retraining or upon ≥ 50 corrections; promote only if macro-F1 improves without > 2% regression on top-20 categories
- M2: Weekly per-user Isolation Forest threshold recalibration from dismissal feedback
- M3: Monthly retrain from updated per-category spending totals
- Business metrics monitored: user-correction rate, average time-to-correction

---

## Deployment & Infrastructure

### Chameleon Cloud
- All runs executed on Chameleon inside containers
- Resource naming: include project ID (e.g. `proj99`) as suffix
- MLflow service running on Chameleon, browsable by course staff
- Object storage for large datasets and model checkpoints
- Block storage for small application state
- Floating IP on one compute instance as jump host

### Training Infrastructure
- All training tracked in MLflow (config params, model quality metrics, training cost metrics, environment info)
- One training script per framework per prediction task
- Candidates/hyperparameters selected via YAML config files
- Data-snapshot hashes and git commits logged per MLflow run

### Docker Containers
- `Dockerfile.m1` — M1 training container
- `Dockerfile.m2` — M2 training container
- `Dockerfile.m3` — M3 training container
- `serving/m1_baseline/Dockerfile` — M1 baseline serving
- `serving/m1_onnx/Dockerfile` — M1 ONNX serving
- `serving/m1_onnx_multiworker/Dockerfile` — M1 multi-worker serving
- `serving/m1_rayserve_bonus/Dockerfile` — M1 Ray Serve

---

## System Integration (Apr 20 milestone)

### Requirements
- Single integrated ML system running on Chameleon
- End-to-end plumbing: production data → feature computation → inference → feedback capture → retraining → evaluation → packaging → deployment → rollback/update
- Workflows automated with minimal human intervention (manual approval for canary → production OK, but not SSH + manual commands)
- ML features implemented in ActualBudget's UI:
  - Categorization: auto-filled field or ranked suggestion dropdown
  - Anomaly: warning badges requiring explicit user confirmation/dismissal
  - Forecasting: suggested envelope budget targets in budget view
- All interactions logged as feedback for next retraining cycle
- De-duplicated shared infrastructure (single MLflow, single monitoring stack, single training data bucket)

### Per-Role Responsibilities (System Integration)
- **Training**: Model quality evaluation with gates; registered models only if passing quality checks
- **Serving**: Monitor deployed model behavior (outputs, operational metrics, user feedback); triggers for promotion/rollback
- **Data**: Data quality evaluation at ingestion, training set compilation, and live inference; drift monitoring

### Safeguarding Plan
Must implement concrete mechanisms for: fairness, explainability, transparency, privacy, accountability, robustness.

### Environments (3-person team)
- CI/CD with retraining and model promotion with well-justified rules
- Automated model rollback if production system degrades

---

## ActualBudget Integration Points

### Key Files/Directories in ActualBudget
- `actual/packages/loot-core/` — Core business logic, DB, server, rules, transactions, budget
- `actual/packages/desktop-client/src/components/` — React UI components
  - `budget/` — Budget page components (where M3 forecasts surface)
  - `transactions/` — Transaction table (where M1 categorization integrates)
  - `accounts/` — Account views
- `actual/packages/sync-server/` — Sync server (Node.js/Express)
- `actual/packages/api/` — API layer

### ActualBudget Tech Stack
- Frontend: React + TypeScript
- Backend: Node.js, SQLite (local-first), Express sync server
- Build: Vite, Yarn workspaces
- Desktop: Electron
- Database migrations in `loot-core/migrations/`

---

## AI Coding Workflow (for ActualBudget integration)

### Best Practices
1. Always start from a working system state
2. Change one smallest meaningful observable unit at a time
3. Isolate each unit in its own branch and merge when validated
4. Define success (pass/fail) before coding
5. Put the agent in the eval loop (push, deploy, verify)
6. Optimize for human review throughput

### OpenCode Configuration
- Use Portkey via NYU AI gateway (requires NYU VPN)
- Models: Claude Haiku 4.5, Sonnet 4.5/4.6, Opus 4.5/4.6
- Budget: $20/week per student
- Playwright MCP available for browser-based validation

### AI Use Policy
- Human owns the design; LLM helps implement
- Must understand all generated code
- No silent design changes by LLM
- Disclosure required on commits with LLM-generated code
- All reports/documentation written by humans only
- Integration with ActualBudget codebase is exempt from "understand everything" policy — AI coding agents explicitly encouraged for this part

---

## Dataset Schemas

### moneydata.csv (raw)
```
Transaction Date, Transaction Type, Transaction Description, Debit Amount, Credit Amount, Balance
25/07/2022, BP, SAVE THE CHANGE, 3.11, , 541.43
```

### moneydata_labeled.csv
```
date, transaction_type, merchant, debit_amount, credit_amount, balance, amount, project_category
2022-07-25, BP, SAVE THE CHANGE, 3.11, 0.0, 541.43, -3.11, misc
```

### synthetic_transactions.csv
```
synthetic_user_id, source_household_id, persona_cluster, date, merchant, project_category,
transaction_type, amount, is_synthetic, transaction_id, abs_amount, day_of_week, day_of_month,
month, log_abs_amount, abs_amount_rounded, repeat_count, is_recurring_candidate
```

### synthetic_users.csv
```
synthetic_user_id, source_household_id, [13 category spend columns], AGE_REF, SEX_REF,
FAM_SIZE, FINLWT21, user_scale, persona_cluster
```

### ces_household_category_spend.csv
```
household_id, category, annual_spend
```

### Project Categories (20 classes)
charity, education, entertainment, gas, groceries, healthcare, housing, misc, personal_care, restaurants, shopping, transport, utilities, cash_transfers, and others from MoneyData mapping.
