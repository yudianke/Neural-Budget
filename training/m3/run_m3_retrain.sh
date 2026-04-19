#!/usr/bin/env bash
# M3 Monthly Retrain Script
# Runs on the 1st of each month via cron on the training VM.
# Ground truth = actual spend from the previous month (already in the batch data).
#
# Cron entry (add with: crontab -e):
#   0 2 1 * * /home/cc/Neural-Budget/training/m3/run_m3_retrain.sh >> /var/log/m3_retrain.log 2>&1

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/training/m3"
M3_SERVING_URL="${M3_SERVING_URL:-http://localhost:8002}"
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://129.114.27.211:8000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-0fc136376b7c47528dfd06a09d12ccbd}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-b52fcbb61618453aacc6ababb530031c}"
MLFLOW_S3_ENDPOINT_URL="${MLFLOW_S3_ENDPOINT_URL:-https://chi.tacc.chameleoncloud.org:7480}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY MLFLOW_S3_ENDPOINT_URL

echo "[M3-retrain] $(date) Starting monthly retrain"
echo "[M3-retrain] Data source: s3://neural-budget-data-proj16/processed/batch_datasets"

# Step 1: Train new model — reads forecasting_train.csv directly from S3
# (batch_pipeline.py uploads to S3 after each run; training reads from there)
echo "[M3-retrain] Training M3 forecast model..."
MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
  python3 "$SCRIPT_DIR/train_m3.py"

# Step 2: If new version registered, reload the inference service
echo "[M3-retrain] Checking for new version and reloading serving..."
curl -s -X POST "$M3_SERVING_URL/admin/reload" \
  -H 'Content-Type: application/json' | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('[M3-retrain] Reload response:', d)
" || echo "[M3-retrain] Reload request failed (service may be down)"

echo "[M3-retrain] $(date) Done"
