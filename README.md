# NeuralBudget — Smart Finance Assistant

An optional ML layer on top of [ActualBudget](https://github.com/actualbudget/actual), a self-hosted open-source personal finance app. Deployed on Chameleon Cloud. Target: 10–300 users per instance. ActualBudget remains fully functional without the ML features.

**Team:** Chetan Kodeboyina (Training), Yudian Ke (Serving), Adib Zandkarim (Data Pipeline)

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/yudianke/Neural-Budget.git
cd Neural-Budget

# 2. Start ML stack (M1, M2, M3, daemons, Prometheus, Grafana)
docker-compose up -d --build

# 3. Start ActualBudget with HTTPS
docker stop actual-development 2>/dev/null; docker rm actual-development 2>/dev/null
docker build -t actual-development ./actual
docker run -d \
  --name actual-development \
  --network neural-budget_ml-net \
  -e HTTPS=true \
  -e M1_SERVICE_URL=http://m1-serving:8001 \
  -e M2_SERVICE_URL=http://m2-serving:8003 \
  -e M3_SERVICE_URL=http://m3-serving:8002 \
  -p 3001:3001 \
  -v ~/Neural-Budget/actual:/app \
  --restart unless-stopped \
  actual-development
```

Wait ~3 minutes for Vite build, then open **https://\<VM-IP\>:3001** (accept the self-signed certificate warning).

---

## ML Features

| Feature | Model | Status |
|---|---|---|
| M1 — Transaction Auto-Categorization | TF-IDF + XGBoost | Production |
| M2 — Anomaly Detection | Isolation Forest + Rules | Production |
| M3 — Budget Forecasting | HistGradientBoosting | Production |

---

## M1 — Transaction Auto-Categorization

### What it does
Predicts the spending category from payee name + amount when a transaction is imported. Auto-fills the category if confidence ≥ 0.6; shows a ranked top-3 suggestion dropdown if < 0.6.

### Model
- **Features:** TF-IDF character n-grams (char_wb, config-driven range e.g. 1–3 with 1000 features for the baseline config) on normalized merchant, plus `log_amount`, `day_of_week`, `day_of_month`
- **Algorithm:** XGBoost classifier, trained inside a Ray `@ray.remote` task so the orchestration layer matches the production retrain-daemon. Sparse CSR features are preserved end-to-end (missing-value semantics intact between train and serve)
- **Training data:** Synthetic categorization CSV in Chameleon Swift (`neural-budget-data-proj16/processed/categorization_train.csv` ≈ 1.35M rows, `categorization_eval.csv` ≈ 339k rows). The older MoneyData sklearn baseline still lives under `training/m1/` as a fallback but is not retrained.
- **Output:** `predicted_category`, `confidence`, `top_3_suggestions`, `auto_fill` flag

### Training

```bash
# Local (requires ray[train], xgboost, mlflow, boto3 on PATH)
MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
AWS_ACCESS_KEY_ID=<key> AWS_SECRET_ACCESS_KEY=<secret> \
AWS_ENDPOINT_URL=https://chi.tacc.chameleoncloud.org:7480 \
MLFLOW_S3_ENDPOINT_URL=https://chi.tacc.chameleoncloud.org:7480 \
python3 training/m1_ray/train_m1_ray.py \
  --mode bootstrap --config training/m1_ray/config_m1_ray.yaml

# Docker (production retrain-daemon image)
docker compose up -d --build retrain-daemon
docker exec <retrain-daemon-container> \
  python3 /app/training/m1_ray/train_m1_ray.py \
  --mode bootstrap --config /app/training/m1_ray/config_m1_ray.yaml

# Hyperparam sweep across config_m1_ray{,_v2,_v3,_v4}.yaml
bash training/m1_ray/run_hypersearch.sh
```

Promotion gates (all must pass to register):
1. Absolute gate — macro-F1 ≥ `quality_gate_macro_f1` (default 0.65).
2. Per-category regression — no category's F1 may drop by more than 2% relative vs the previously registered version.
3. Improvement — bootstrap mode skips improvement check; retrain mode requires improvement over previous registered macro-F1.

Failing any gate skips registration (no rollback needed — the previous version stays in place).

### Retraining
- **Schedule:** Weekly, or immediately upon ≥ 50 user corrections
- **Trigger:** `retrain-daemon` container polls every 5 minutes, checks the feedback log
- **Rollback:** After 24h, if correction rate on the new version rises >15%, daemon calls `/admin/reload?version=<old>` to revert

### Serving

```bash
# Baseline (CPU, FastAPI)
cd serving/m1_baseline
docker build -t m1-serving .
docker run -p 8001:8001 \
  -e MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
  m1-serving
```

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health + loaded model version |
| POST | `/predict/category` | Categorize a transaction |
| POST | `/feedback` | Log a user correction |
| GET | `/metrics` | Prometheus metrics |
| GET | `/metrics/feedback` | Aggregate feedback stats |
| GET | `/metrics/feedback/since/{model_version}` | Feedback stats scoped to a specific model version (used by retrain-daemon rollback check) |
| POST | `/admin/reload` | Hot-reload model from MLflow |

**Serving variants:**
- `serving/m1_baseline/` — FastAPI, CPU reference
- `serving/m1_onnx/` — ONNX-optimized
- `serving/m1_onnx_multiworker/` — Gunicorn multi-worker ONNX
- `serving/m1_rayserve_bonus/` — Ray Serve

### Latency target
< 0.5s per transaction (synchronous, inline with import)

### MLflow
Experiment: `m1-categorization` — tracks macro-F1, per-category F1, training time, data hash, git commit.
Registry: `m1-ray-categorization`

---

## M2 — Anomaly Detection

### What it does
Scores every imported transaction asynchronously (< 5s) and displays a warning badge if the transaction is anomalous. Users can dismiss false positives. Dismissals feed back into a weekly retraining cycle that adjusts the model's sensitivity automatically.

Cold-start guard: inactive until the user has ≥ 50 transactions.

### Model
- **ML layer:** IsolationForest exported to ONNX (6 features: `abs_amount`, `repeat_count`, `is_recurring_candidate`, `user_txn_index`, `user_mean_abs_amount_prior`, `user_std_abs_amount_prior`)
- **Rule layer** (computed in loot-core): `duplicate_within_24h`, `subscription_jump`
- **Badge types:** `duplicate` · `price_jump` · `spike`
- **Output:** `anomaly_score`, `is_anomaly`, `badge_type`, `rule_flags`, `model_version`

### Training

```bash
# Docker (Chameleon)
docker build -f training/m2/Dockerfile.m2 -t m2-training .
docker run --rm \
  -e MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  m2-training
```

MLflow experiment: `m2-anomaly` — tracks contamination, dismiss_rate, training time, git commit.
Registry: `m2-anomaly`

### Retraining

The `m2-retrain-daemon` container drives the full retrain + rollback cycle automatically (polls every 5 minutes).

**Three retrain triggers:**
1. ≥ 30 new dismissals since last retrain
2. Weekly (Sunday), if last retrain was ≥ 6 days ago
3. Urgent: dismiss rate > 50%

**Contamination adjustment:** if dismiss rate > 30%, `contamination` decreases by 0.01 (floor 0.01), reducing false-positive sensitivity before retraining.

**Rollback:** 24h after a retrain, if the new model's dismiss rate > previous × 1.20, daemon calls `/admin/reload?version=<old>` to revert automatically.

**Manual / forced retrain (for testing):**
```bash
# Trigger retrain immediately
docker exec m2-retrain-daemon python3 training/m2/retrain_daemon.py --force-retrain

# Or run the retrain script directly
docker exec m2-retrain-daemon bash /app/training/m2/run_m2_retrain.sh
```

### Serving

```bash
cd serving/m2_onnx_multiworker
docker build -t m2-serving .
docker run -p 8003:8003 \
  -e MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
  -e M2_FEEDBACK_LOG_PATH=/data/feedback/m2_feedback.jsonl \
  -e PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc_m2 \
  m2-serving
```

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health + loaded model version |
| POST | `/predict/anomaly` | Score a transaction for anomalies |
| POST | `/feedback` | Log a dismiss or confirm signal |
| GET | `/metrics/feedback` | Aggregate dismiss stats (used by daemon) |
| GET | `/metrics/feedback/since/{version}` | Version-filtered stats for rollback check |
| POST | `/admin/reload` | Hot-reload ONNX model from disk |
| GET | `/metrics` | Prometheus metrics (multiprocess-safe) |

**Rollback a specific version:**
```bash
curl -X POST http://localhost:8003/admin/reload?version=1
```

### Latency target
< 5s asynchronous — M2 is fire-and-forget, never blocks the transaction save path.

### UI Integration
- `AnomalyBadge` renders inside the payee cell of every transaction row
- Badge label is localized: `Possible duplicate` · `Subscription price jump` · `Unusual amount`
- Dismiss `×` button writes `anomaly_dismissed=1` to SQLite and sends a fire-and-forget `POST /feedback` to the M2 serving layer
- Badge auto-hides after dismiss without a page reload

### MLflow
- Experiment: `m2-anomaly` — tracks contamination, dismiss_rate, training data hash
- Registry: `m2-anomaly` — currently on v1
- Daemon monitor logs: retrain, rollback, and no-improvement events

---

## M3 — Budget Forecasting

### What it does
Predicts the **current in-progress month**'s spend per category, using complete prior-month history as lag features. Shown in the ActualBudget dashboard as a ForecastCard, compared against the user's current-month budget. Users can apply the forecasts as current-month budget targets with one click when a month hasn't been budgeted yet.

The current in-progress month is excluded from the lag history (a partial month would bias `lag_1` low), so the model's target is the month immediately after the last complete month — i.e. the month you're currently in.

### Model
- **Features:** Monthly spend per category with lag features (lag_1 through lag_6), rolling mean/std over 3 and 6 months, month-of-year trig encoding (sin/cos), quarter, is_q4, history_month_count
- **Algorithm:** HistGradientBoostingRegressor (scikit-learn 1.6.1)
- **Training data:** Synthetic transactions generated from MoneyData + BLS CES household survey (245,652 rows on Chameleon S3)
- **Output:** Point forecast per category (dollars), used to populate the ForecastCard and optionally set budget targets

### Training

```bash
# Local (uses local fallback data if no S3 credentials)
cd training/m3
python3 train_m3.py

# Docker (reads from S3)
docker build -f Dockerfile.m3 -t m3-training .
docker run --rm \
  -e MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  -e MLFLOW_S3_ENDPOINT_URL=https://chi.tacc.chameleoncloud.org:7480 \
  m3-training
```

**Promotion gates (all must pass to register):**
1. Overall MAE ≤ `M3_GATE_MAE` threshold (default 150.0)
2. Overall MAE must improve vs previous registered version
3. No single category may regress >30% in MAE vs previous version (`M3_CATEGORY_REGRESSION_MAX`)

Each run logs: overall MAE, median per-category MAE, per-category MAE (as `mae_<category>`), training time, data MD5 hash, git commit.

### Retraining

The `m3-monitor-daemon` container drives the full retrain + rollback cycle automatically.

**Monthly schedule:**
- **Day 2:** triggers `run_m3_retrain.sh` → `train_m3.py` → MLflow registration → `/admin/reload` on m3-serving → verifies new version is live
- **Day 16:** calls `/metrics/forecast-accuracy` on m3-serving, compares deployed model MAE vs previous version MAE. If new MAE > old MAE × 1.20, auto-rollback via `/admin/reload?version=<old>`

**Manual / forced retrain (for testing):**
```bash
# Force retrain immediately without waiting for day 2
M3_FORCE_RETRAIN=1 CHECK_INTERVAL_SECONDS=30 docker compose up -d m3-monitor-daemon

# Or directly run the retrain script
docker exec m3-monitor-daemon bash /app/training/m3/run_m3_retrain.sh
```

**Reset daemon state to allow re-retrain this month:**
```bash
docker exec m3-monitor-daemon python3 -c "
import json; p='/data/m3_state/monitor_state.json'
s=json.load(open(p)); s['last_retrain_year_month']=None
json.dump(s,open(p,'w'))
"
```

### Serving

```bash
cd serving/m3
docker build -t m3-serving .
docker run -p 8002:8002 \
  -e MLFLOW_TRACKING_URI=http://129.114.25.192:8000 \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  -e MLFLOW_S3_ENDPOINT_URL=https://chi.tacc.chameleoncloud.org:7480 \
  m3-serving
```

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health + loaded model version |
| POST | `/forecast/features` | Forecast next-month spend from feature rows |
| GET | `/metrics/forecast-accuracy` | Per-category MAE vs actuals for a model version |
| POST | `/admin/reload` | Hot-reload model from MLflow (optionally pin a version) |
| GET | `/metrics` | Prometheus metrics |

**Rollback a specific version:**
```bash
curl -X POST http://localhost:8002/admin/reload?version=3
```

### Latency target
Background batch job — no strict latency requirement. Forecasts are pre-computed and cached.

### MLflow
- Experiment: `m3-forecast` (experiment ID 6 on Chameleon) — tracks all metrics and gates
- Registry: `m3-forecast` — currently on v5
- Daemon monitor: `m3-retrain-monitor` (experiment ID 9) — logs retrain, eval, rollback events

### UI Integration
The ForecastCard in the ActualBudget dashboard shows:

- Per-category forecast (current in-progress month) with comparison bars (forecast vs current-month budget vs last month)
- Gap chip: forecast minus current-month budget target (red = over, green = under)
- **"Use forecasts as budgets"** button — writes M3 forecasts into the current month's budget sheet in one click (only fills categories where the current-month budget is $0). The banner fires only when the current month has no budgets set yet.
- Refresh button to re-fetch forecasts on demand

---

## Full Stack

### Step 1 — Start ML stack + daemons + monitoring

```bash
# Clone and enter repo
git clone https://github.com/yudianke/Neural-Budget.git
cd Neural-Budget

# Create .env (copy from .env.example and fill in credentials)
cp .env.example .env

# Start everything
docker-compose up -d --build
```

### Step 2 — Start ActualBudget (run separately on VM due to docker-compose version)

```bash
docker stop actual-development 2>/dev/null; docker rm actual-development 2>/dev/null
docker build -t actual-development ./actual
docker run -d \
  --name actual-development \
  --network neural-budget_ml-net \
  -e HTTPS=true \
  -e M1_SERVICE_URL=http://m1-serving:8001 \
  -e M2_SERVICE_URL=http://m2-serving:8003 \
  -e M3_SERVICE_URL=http://m3-serving:8002 \
  -p 3001:3001 \
  -v ~/Neural-Budget/actual:/app \
  --restart unless-stopped \
  actual-development
```

> **Note:** `HTTPS=true` is required when accessing ActualBudget from an external browser. Without it, the browser blocks `SharedArrayBuffer` which ActualBudget requires. Access via `https://` and accept the self-signed certificate warning.

### Required env vars (in `.env` file — do not commit)

```bash
MLFLOW_TRACKING_URI=http://129.114.25.192:8000
AWS_ACCESS_KEY_ID=<chameleon-object-store-key>
AWS_SECRET_ACCESS_KEY=<chameleon-object-store-secret>
MLFLOW_S3_ENDPOINT_URL=https://chi.tacc.chameleoncloud.org:7480
```

Services:
| Container | Port | Description |
|---|---|---|
| `actual-development` | 3001 | ActualBudget UI — access via `https://` |
| `m1-serving` | 8001 | M1 categorization inference |
| `retrain-daemon` | — | M1 weekly retrain + rollback |
| `m2-serving` | 8003 | M2 anomaly detection inference |
| `m2-retrain-daemon` | — | M2 feedback-driven retrain + rollback |
| `m3-serving` | 8002 | M3 forecast inference |
| `m3-monitor-daemon` | — | M3 monthly retrain + rollback |
| `prometheus` | 9090 | Metrics scraping |
| `grafana` | 3000 | Dashboards (admin pw: `neuralbudget`) |

### URLs (Chameleon VM: `129.114.27.248`)

| Service | URL |
|---|---|
| ActualBudget | `https://129.114.27.248:3001` |
| Grafana | `http://129.114.27.248:3000` (admin / neuralbudget) |
| Prometheus | `http://129.114.27.248:9090` |
| MLflow | `http://129.114.25.192:8000` |

MLflow UI: http://129.114.25.192:8000

---

## Repository Structure

```
Neural-Budget/
├── actual/                    # ActualBudget app (TypeScript/React) — ML integrated
├── data_pipeline/             # Synthetic data generation + batch feature pipeline
│   └── processed/             # Local fallback training data (committed)
├── datasets/                  # Raw source datasets
├── serving/
│   ├── m1_baseline/           # M1 FastAPI serving (CPU)
│   ├── m1_onnx/               # M1 ONNX optimized
│   ├── m1_onnx_multiworker/   # M1 Gunicorn multi-worker
│   ├── m1_rayserve_bonus/     # M1 Ray Serve
│   ├── m2_onnx_multiworker/   # M2 Gunicorn multi-worker ONNX + feedback loop
│   └── m3/                    # M3 forecast serving
├── training/
│   ├── m1/                    # Legacy sklearn baseline (source of the m1-categorization fallback model)
│   ├── m1_ray/                # Active M1 training: XGBoost in a @ray.remote task + retrain daemon
│   ├── m2/                    # M2 Isolation Forest training
│   └── m3/                    # M3 HistGB training + monitor daemon
├── monitoring/                # Prometheus + Grafana config
├── docker-compose.yml
├── AGENTS.md                  # Project spec and team roles
└── README.md
```
