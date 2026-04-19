# NeuralBudget — System-Wide Safeguarding Plan

This document describes concrete safeguarding mechanisms implemented across all three ML models (M1, M2, M3) in six dimensions: **Fairness, Explainability, Transparency, Privacy, Accountability, Robustness**. It serves as the unified reference for the NeuralBudget MLOps project.

---

## Overview

| Dimension | M1 (Categorization) | M2 (Anomaly) | M3 (Forecasting) |
|---|---|---|---|
| **Fairness** | Per-category F1 gate + underperformance flag | Dismiss rate feedback loop | Category regression gate |
| **Explainability** | Top-50 TF-IDF n-grams logged to MLflow | Badge type + rule flags shown in UI | Feature importance via lag/rolling features |
| **Transparency** | Confidence score shown to user | Badge label + dismiss button | Forecast vs budget comparison UI |
| **Privacy** | PII scan on merchant strings | Only aggregate stats sent to serving | No demographic features in model |
| **Accountability** | Git SHA + data MD5 logged per run | Dismiss JSONL + MLflow retrain events | Git SHA + data MD5 logged per run |
| **Robustness** | Low-confidence rate metric + rollback gate | Cold-start guard (≥50 txns) + timeout | 3-gate promotion + MAE rollback |

---

## 1. Fairness

### M1 — Category-level performance floor
**Implementation:** `training/m1_ray/safeguarding.py:35–49`

Every training run flags spending categories where F1 score falls below `FAIRNESS_F1_FLOOR = 0.40`. Underperforming categories are logged as a comma-separated MLflow tag `safeguard_fairness_underperforming`. A run that degrades a previously passing category triggers the per-category regression gate and is blocked from promotion.

```python
# training/m1_ray/safeguarding.py:35
def _check_fairness(y_test, y_pred, le):
    """Flag categories with F1 < FAIRNESS_F1_FLOOR."""
```

**Per-category regression gate:** `training/m1_ray/train_m1_ray.py:428–440`
No category may regress more than 2% relative F1 vs the previous registered version. If any does, the model is not promoted.

### M2 — Dismiss rate as fairness signal
**Implementation:** `training/m2/retrain_daemon.py:192–210`

A high dismiss rate (>30%) signals that the anomaly threshold is too aggressive — disproportionately flagging normal transactions. The contamination parameter is automatically reduced by 0.01 per high-dismiss retrain, down to a floor of 0.01, reducing false-positive rates system-wide.

### M3 — Category-level MAE regression gate
**Implementation:** `training/m3/train_m3.py:462–486`

No individual spending category may regress more than 30% in MAE vs the previous registered version (`M3_CATEGORY_REGRESSION_MAX=0.30`). A model that improves overall but degrades a specific category (e.g. housing, utilities) is blocked from promotion.

---

## 2. Explainability

### M1 — Top features artifact
**Implementation:** `training/m1_ray/safeguarding.py:55–86`

Every training run logs a `explainability/top_features.json` artifact to MLflow containing:
- Top 50 globally discriminative TF-IDF n-grams ranked by IDF weight
- All class names (spending categories)
- Numeric feature names (log_amount, day_of_week, etc.)

Viewable at: `http://129.114.27.211:8000 → Experiments → m1-ray-categorization → latest run → Artifacts → explainability/top_features.json`

### M2 — Badge type + rule flags exposed to user
**Implementation:** `serving/m2_onnx_multiworker/app.py`, `actual/packages/desktop-client/src/components/transactions/TransactionsTable.tsx`

Every anomaly response includes `rule_flags` (which deterministic rules triggered) and `badge_type` (the specific anomaly kind). The UI renders a localized human-readable label:
- `duplicate_within_24h` → "Possible duplicate"
- `subscription_jump` → "Subscription price jump"
- `amount_spike` → "Unusual amount"

Users are never shown a raw score — only a specific, actionable reason.

### M3 — Forecast breakdown per category
**Implementation:** `actual/packages/desktop-client/src/components/reports/ForecastCard.tsx`

The ForecastCard shows per-category forecasts with comparison bars (forecast vs current budget vs last month spend) and a gap chip (forecast − budget). Users can see exactly which categories are expected to change and by how much.

---

## 3. Transparency

### M1 — Confidence score surfaced to user
**Implementation:** `actual/packages/loot-core/src/server/transactions/ml-service.ts`

- If confidence ≥ 0.6: category is auto-filled with a visual indicator
- If confidence < 0.6: no auto-fill; top-3 suggestions shown as a ranked dropdown
- Threshold (`CONFIDENCE_THRESHOLD = 0.6`) is defined in one place and consistent between serving and UI

Users always know when M1 is uncertain. The system never silently forces a low-confidence prediction.

### M2 — Degraded mode disclosure
**Implementation:** `serving/m2_onnx_multiworker/app.py` `/health` endpoint

```json
{"status": "ok", "model_version": "m2_isolation_forest_v1"}
```

If M2 is down or unreachable, the transaction save is completely unaffected (fire-and-forget). No badge is shown rather than a misleading one. Users are never told a transaction is anomalous due to a service error.

### M3 — Forecast vs actuals monitoring
**Implementation:** `serving/m3/m3_inference_service.py:301–349`

`GET /metrics/forecast-accuracy` computes per-category MAE vs real actuals (when `M3_ACTUALS_URL` is set). This is the basis for automated rollback and is also visible in the Grafana M3 dashboard.

### All models — Grafana dashboards
Each model has a dedicated Grafana dashboard (`monitoring/grafana/dashboards/`) showing request rate, latency, error rate, model version, and model-specific metrics (confidence distribution, anomaly rate, dismiss rate, forecast MAE). Operational transparency is continuous.

---

## 4. Privacy

### M1 — PII scan on training data
**Implementation:** `training/m1_ray/safeguarding.py:118–148`

Before every training run, the merchant name column is scanned for strings matching a 12–19 digit card-number pattern (`\b\d{12,19}\b`). If any are found, an MLflow warning tag is set: `safeguard_privacy_pii_check=WARN_N_potential_card_numbers`. The training run is not blocked (the pattern is a heuristic, not a guarantee) but the finding is logged for review.

```python
CARD_NUMBER_RE = re.compile(r"\b\d{12,19}\b")
```

### M2 — Minimal data sent to serving
**Implementation:** `actual/packages/loot-core/src/server/transactions/ml-service-m2.ts`

The M2 serving layer receives only: `abs_amount`, `repeat_count`, `is_recurring_candidate`, `user_txn_index`, `user_mean_abs_amount_prior`, `user_std_abs_amount_prior`, `duplicate_within_24h`, `subscription_jump`. No payee names, descriptions, or account identifiers are sent. Feedback payloads include only the merchant string (already visible in the UI) and the anomaly score — no account numbers, balances, or user PII.

### M3 — No demographic features
**Implementation:** `serving/m3/m3_inference_service.py:140–145`

The `ForecastFeatureRow` schema intentionally excludes demographic features (`persona_cluster`, `AGE_REF`, household income proxies). These were present in the BLS CES training data but are not available from ActualBudget transaction history and would constitute demographic profiling. The model is trained exclusively on spending behaviour, not user identity.

### All models — Local-first architecture
ActualBudget is self-hosted. All transaction data stays in the user's local SQLite database. ML services receive only the minimum features needed for inference — no raw transaction history is sent to external servers. MLflow receives only training metrics and model artifacts, not user data.

---

## 5. Accountability

### M1 — Git SHA + data hash per training run
**Implementation:** `training/m1_ray/safeguarding.py:154–175`

Every M1 training run logs:
- `safeguard_git_commit`: short SHA of the HEAD commit at training time
- `safeguard_data_hash_md5`: MD5 of the training CSV

This makes every registered model version traceable to the exact code and data it was trained on.

### M2 — Dismiss feedback JSONL + MLflow retrain events
**Implementation:** `serving/m2_onnx_multiworker/app.py`, `training/m2/retrain_daemon.py`

Every dismiss action is appended to `/data/feedback/m2_feedback.jsonl` with:
- `transaction_id`, `feedback_type`, `badge_type`, `anomaly_score`, `rule_flags`
- `logged_at` (UTC timestamp), `model_version`, `model_name`

Every retrain and rollback event is logged to MLflow experiment `m2-retrain-daemon` with contamination, dismiss_rate, trigger type, and version numbers. The full history of model sensitivity adjustments is auditable.

### M3 — Git SHA + data hash + gate results per training run
**Implementation:** `training/m3/train_m3.py:407–409`

Every M3 training run logs `git_commit` and `data_hash_md5` tags to MLflow, along with gate results (overall MAE, per-category MAE, gate passed/failed, register reason). Rollback events are logged to the `m3-retrain-monitor` MLflow experiment.

### All models — Model registry with versioning
All three models are registered in MLflow Model Registry with semantic version numbers. Every version records: training timestamp, data hash, quality gate results, and the reason for registration or rejection. No model is deployed without passing its gate.

---

## 6. Robustness

### M1 — Low-confidence rate metric
**Implementation:** `training/m1_ray/safeguarding.py:92–112`

After evaluation, the fraction of test predictions with max softmax probability < 0.6 is logged as `low_confidence_rate`. If this exceeds 20%, a warning tag `safeguard_robustness_warning=true` is set. A high low-confidence rate indicates the model is uncertain on a large fraction of inputs and may need more training data.

**Service timeout:** `actual/packages/loot-core/src/server/transactions/ml-service.ts:197`
M1 calls abort after 5 seconds. If the service is slow or down, the transaction save completes normally with no category predicted.

### M2 — Cold-start guard + timeout
**Implementation:** `actual/packages/loot-core/src/server/transactions/ml-service-m2.ts:35–37, 176–182`

M2 is inactive until the user has ≥ 50 transactions (`MIN_TRANSACTIONS = 50`). Below this threshold, IsolationForest has insufficient history to establish a normal baseline and would produce unreliable anomaly scores. M2 calls abort after 4 seconds. The transaction save is never blocked by M2.

**Contamination floor:** `training/m2/retrain_daemon.py:192–210`
Contamination is never reduced below 0.01, preventing the model from becoming so insensitive that it never flags real anomalies.

### M3 — Three-gate promotion
**Implementation:** `training/m3/train_m3.py:428–486`

A new M3 model must pass all three gates before registration:
1. **Absolute gate:** overall MAE ≤ `M3_GATE_MAE` (default 150.0)
2. **Improvement gate:** MAE must improve vs previous registered version
3. **Category regression gate:** no category may regress > 30% in MAE

**Automated rollback:** `training/m3/m3_monitor_daemon.py:361–371`
16 days after each retrain, the daemon computes the production MAE of the new version. If it is more than 20% worse than the previous version (`M3_ROLLBACK_MAE_DELTA=1.20`), the daemon automatically reverts to the previous version via `POST /admin/reload?version=<old>`.

**Graceful degradation:** `serving/m3/m3_inference_service.py:242–244`
If the model fails to load, M3 returns an empty forecast list rather than an error. The ActualBudget UI shows a loading state rather than crashing.

---

## Gaps and Limitations

| Gap | Severity | Notes |
|---|---|---|
| M2 and M3 training have no dedicated safeguarding module | Medium | Quality gates exist but fairness/privacy/explainability checks are not run at training time |
| M2 PII scan not implemented | Medium | M1 scans merchant strings for card numbers; M2 does not scan its training data |
| `M3_ACTUALS_URL` not wired to a real actuals source | Medium | Without it, rollback uses training-eval MAE as proxy (not production MAE). Daemon falls back gracefully. |
| M1 non-Ray training (`train_m1.py`) has no safeguarding | Low | Only `train_m1_ray.py` calls `safeguarding.py`; the baseline script does not |
| Explainability for M2 is rule-level only | Low | No feature importance artifact logged to MLflow for the IsolationForest |
| SQL injection comment in `aql/exec.ts:15` | Low | A TODO comment, not an implemented guard |

---

## Key Files

| File | Role |
|---|---|
| `training/m1_ray/safeguarding.py` | Fairness, explainability, robustness, privacy, accountability checks for M1 |
| `training/m1_ray/train_m1_ray.py` | Calls `run_safeguarding_checks()` + per-category regression gate |
| `training/m2/retrain_daemon.py` | Contamination adjustment from dismiss rate, rollback window |
| `training/m3/train_m3.py` | Three-gate promotion, per-category regression gate |
| `training/m3/m3_monitor_daemon.py` | Automated MAE-based rollback |
| `serving/m2_onnx_multiworker/app.py` | Feedback JSONL, dismiss rate gauge |
| `actual/packages/loot-core/src/server/transactions/ml-service.ts` | Confidence threshold, M1 timeout, fail-safe |
| `actual/packages/loot-core/src/server/transactions/ml-service-m2.ts` | Cold-start guard, M2 timeout, dismiss feedback |
| `monitoring/grafana/dashboards/` | Operational transparency — m1, m2, m3 dashboards |
