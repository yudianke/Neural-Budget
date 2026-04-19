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

echo "[M3-retrain] $(date) Starting monthly retrain"

# Step 1: Regenerate batch datasets with latest data
echo "[M3-retrain] Running batch pipeline..."
cd "$REPO_ROOT"
python3 data_pipeline/batch_pipeline.py

# Step 2: Train new model with quality gate + MLflow registration
echo "[M3-retrain] Training M3 forecast model..."
MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
  python3 "$SCRIPT_DIR/train_m3_v2.py"

# Step 3: If new version registered, reload the inference service
echo "[M3-retrain] Checking for new version and reloading serving..."
curl -s -X POST "$M3_SERVING_URL/admin/reload" \
  -H 'Content-Type: application/json' | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('[M3-retrain] Reload response:', d)
" || echo "[M3-retrain] Reload request failed (service may be down)"

echo "[M3-retrain] $(date) Done"
