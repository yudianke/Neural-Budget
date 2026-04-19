# NeuralBudget — Smart Finance Assistant

An optional ML layer on top of [ActualBudget](https://github.com/actualbudget/actual), a self-hosted open-source personal finance app. Deployed on Chameleon Cloud. Target: 10–300 users per instance. ActualBudget remains fully functional without the ML features.

**Team:** Chetan Kodeboyina (Training), Yudian Ke (Serving), Adib Zandkarim (Data Pipeline)

---

## ML Features

| Feature | Model | Status |
|---|---|---|
| M1 — Transaction Auto-Categorization | TF-IDF + XGBoost | Production |
| M2 — Anomaly Detection | Isolation Forest + Rules | Training only |
| M3 — Budget Forecasting | HistGradientBoosting | Production |

---

## M1 — Transaction Auto-Categorization

### What it does
Predicts the spending category from payee name + amount when a transaction is imported. Auto-fills the category if confidence ≥ 0.6; shows a ranked top-3 suggestion dropdown if < 0.6.

### Model
- **Features:** TF-IDF character n-grams (n=3–5, 500 features) on payee name, log-amount, day-of-week, day-of-month, account type, historical majority category for payee
- **Algorithm:** XGBoost classifier (also Ray Train variant for distributed training)
- **Training data:** MoneyData (6,500+ real UK bank transactions, 2015–2022) + synthetic transactions generated from BLS CES household survey data
- **Output:** `predicted_category`, `confidence`, `top_3_suggestions`, `auto_fill` flag

### Training

```bash
# Local
cd training/m1
python3 train_m1.py --config config_xgb.yaml

# Docker
docker build -f Dockerfile.m1 -t m1-training .
docker run --rm \
  -e MLFLOW_TRACKING_URI=http://129.114.27.211:8000 \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  m1-training
```

Promotion gate: new model must improve macro-F1 without >2% regression on the top-20 categories, otherwise auto-rollback.

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
  -e MLFLOW_TRACKING_URI=http://129.114.27.211:8000 \
  m1-serving
```

**Endpoints:**

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Service health + loaded model version |
| POST | `/predict` | Categorize a transaction |
| POST | `/feedback` | Log a user correction |
| GET | `/metrics` | Prometheus metrics |
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

## M3 — Budget Forecasting

### What it does
Predicts next-month spending per category. Shown in the ActualBudget dashboard as a ForecastCard. Users can apply M3 forecasts as next-month budget targets with one click.

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
  -e MLFLOW_TRACKING_URI=http://129.114.27.211:8000 \
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
  -e MLFLOW_TRACKING_URI=http://129.114.27.211:8000 \
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
- Per-category forecast (next month) with comparison bars (forecast vs budget vs last month)
- Gap chip: forecast minus budget target (red = over, green = under)
- **"Use forecasts as budgets"** button — writes M3 forecasts as next-month budget targets in one click (only fills categories where budget is currently $0)
- Refresh button to re-fetch forecasts on demand

---

## Full Stack

```bash
# Start everything
docker compose up -d --build

# Required env vars (in shell or .env file — do not commit)
export MLFLOW_TRACKING_URI=http://129.114.27.211:8000
export AWS_ACCESS_KEY_ID=<chameleon-object-store-key>
export AWS_SECRET_ACCESS_KEY=<chameleon-object-store-secret>
```

Services:
| Container | Port | Description |
|---|---|---|
| `m1-serving` | 8001 | M1 categorization inference |
| `retrain-daemon` | — | M1 weekly retrain + rollback |
| `m3-serving` | 8002 | M3 forecast inference |
| `m3-monitor-daemon` | — | M3 monthly retrain + rollback |
| `prometheus` | 9090 | Metrics scraping |
| `grafana` | 3000 | Dashboards (admin pw: `neuralbudget`) |

MLflow UI: http://129.114.27.211:8000

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
│   └── m3/                    # M3 forecast serving
├── training/
│   ├── m1/                    # M1 XGBoost training
│   ├── m1_ray/                # M1 Ray Train + retrain daemon
│   ├── m2/                    # M2 Isolation Forest training
│   └── m3/                    # M3 HistGB training + monitor daemon
├── monitoring/                # Prometheus + Grafana config
├── docker-compose.yml
├── AGENTS.md                  # Project spec and team roles
└── README.md
```
