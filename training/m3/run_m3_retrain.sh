#!/usr/bin/env bash
# M3 Monthly Retrain Script
# Runs on the 1st of each month via cron on the training VM, or triggered by
# m3_monitor_daemon.py. Snapshots the current model version before retraining,
# verifies the reload actually took effect, and writes a retrain event JSON for
# the daemon's rollback evaluator.
#
# Cron entry (add with: crontab -e):
#   0 2 1 * * /home/cc/Neural-Budget/training/m3/run_m3_retrain.sh >> /var/log/m3_retrain.log 2>&1

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/training/m3"
M3_SERVING_URL="${M3_SERVING_URL:-http://localhost:8002}"
MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://129.114.26.214:8000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-0fc136376b7c47528dfd06a09d12ccbd}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-b52fcbb61618453aacc6ababb530031c}"
MLFLOW_S3_ENDPOINT_URL="${MLFLOW_S3_ENDPOINT_URL:-https://chi.tacc.chameleoncloud.org:7480}"
M3_STATE_DIR="${M3_STATE_DIR:-/data/m3_state}"
RELOAD_TIMEOUT="${RELOAD_TIMEOUT:-60}"  # seconds to wait for version change after reload

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY MLFLOW_S3_ENDPOINT_URL

mkdir -p "$M3_STATE_DIR"

echo "[M3-retrain] $(date) Starting monthly retrain"
echo "[M3-retrain] Data source: s3://neural-budget-data-proj16/processed/batch_datasets"

# ---------------------------------------------------------------------------
# Step 0: Snapshot the current deployed version (needed for rollback)
# ---------------------------------------------------------------------------
VERSION_BEFORE="unknown"
HEALTH_RESPONSE=$(curl -s --max-time 5 "$M3_SERVING_URL/health" 2>/dev/null || echo "{}")
VERSION_BEFORE=$(echo "$HEALTH_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('model_version') or 'unknown')
except Exception:
    print('unknown')
")
echo "[M3-retrain] Version before retrain: $VERSION_BEFORE"

# ---------------------------------------------------------------------------
# Step 1: Train new model — reads forecasting_train.csv directly from S3
# ---------------------------------------------------------------------------
echo "[M3-retrain] Training M3 forecast model..."
TRAIN_EXIT=0
MLFLOW_TRACKING_URI="$MLFLOW_TRACKING_URI" \
  python3 "$SCRIPT_DIR/train_m3.py" || TRAIN_EXIT=$?

if [ "$TRAIN_EXIT" -ne 0 ]; then
  echo "[M3-retrain] Training script failed with exit code $TRAIN_EXIT — aborting"
  python3 -c "
import json, time, os
state_dir = os.environ.get('M3_STATE_DIR', '/data/m3_state')
with open(f'{state_dir}/last_retrain_event.json', 'w') as f:
    json.dump({
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'status': 'train_failed',
        'version_before': '$VERSION_BEFORE',
        'version_after': None,
        'exit_code': $TRAIN_EXIT,
    }, f, indent=2)
"
  exit "$TRAIN_EXIT"
fi

# ---------------------------------------------------------------------------
# Step 2: Check whether a new version was actually registered in MLflow
# ---------------------------------------------------------------------------
VERSION_AFTER=$(python3 -c "
import os, sys
try:
    from mlflow.tracking import MlflowClient
    client = MlflowClient(tracking_uri='$MLFLOW_TRACKING_URI')
    versions = client.search_model_versions(\"name='m3-forecast'\")
    if not versions:
        print('none')
        sys.exit(0)
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    print(str(latest.version))
except Exception as e:
    print('unknown')
" 2>/dev/null)

echo "[M3-retrain] Latest registered version in MLflow: $VERSION_AFTER"

if [ "$VERSION_AFTER" = "$VERSION_BEFORE" ] || [ "$VERSION_AFTER" = "none" ] || [ "$VERSION_AFTER" = "unknown" ]; then
  echo "[M3-retrain] No new version registered (quality gates not passed or MLflow unreachable) — skipping reload"
  python3 -c "
import json, time, os
state_dir = os.environ.get('M3_STATE_DIR', '/data/m3_state')
with open(f'{state_dir}/last_retrain_event.json', 'w') as f:
    json.dump({
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'status': 'no_new_version',
        'version_before': '$VERSION_BEFORE',
        'version_after': None,
    }, f, indent=2)
"
  echo "[M3-retrain] $(date) Done (no new model registered)"
  exit 0
fi

# ---------------------------------------------------------------------------
# Step 3: Reload the inference service with the new version
# ---------------------------------------------------------------------------
echo "[M3-retrain] Reloading m3-serving with version $VERSION_AFTER ..."
RELOAD_RESPONSE=$(curl -s -X POST "$M3_SERVING_URL/admin/reload" \
  -H 'Content-Type: application/json' 2>/dev/null || echo '{"error":"curl failed"}')
echo "[M3-retrain] Reload response: $RELOAD_RESPONSE"

# ---------------------------------------------------------------------------
# Step 4: Verify the reload actually took effect (poll /health until version changes)
# ---------------------------------------------------------------------------
echo "[M3-retrain] Waiting for serving to load v$VERSION_AFTER (timeout: ${RELOAD_TIMEOUT}s)..."
ELAPSED=0
LOADED_VERSION="unknown"
while [ "$ELAPSED" -lt "$RELOAD_TIMEOUT" ]; do
  sleep 3
  ELAPSED=$((ELAPSED + 3))
  CURRENT_VER=$(curl -s --max-time 3 "$M3_SERVING_URL/health" 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d.get('model_version') or 'unknown')
except Exception:
    print('unknown')
" 2>/dev/null || echo "unknown")
  if [ "$CURRENT_VER" = "$VERSION_AFTER" ]; then
    LOADED_VERSION="$CURRENT_VER"
    echo "[M3-retrain] Confirmed: serving is now running v$LOADED_VERSION"
    break
  fi
  echo "[M3-retrain] Still loading... (${ELAPSED}s, current=${CURRENT_VER})"
done

if [ "$LOADED_VERSION" != "$VERSION_AFTER" ]; then
  echo "[M3-retrain] WARNING: reload timed out — serving may still be running v$VERSION_BEFORE"
  RELOAD_STATUS="timeout"
else
  RELOAD_STATUS="ok"
fi

# ---------------------------------------------------------------------------
# Step 5: Write retrain event for the monitor daemon
# ---------------------------------------------------------------------------
python3 -c "
import json, time, os
state_dir = os.environ.get('M3_STATE_DIR', '/data/m3_state')
with open(f'{state_dir}/last_retrain_event.json', 'w') as f:
    json.dump({
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'status': 'success' if '$RELOAD_STATUS' == 'ok' else 'reload_timeout',
        'version_before': '$VERSION_BEFORE',
        'version_after': '$VERSION_AFTER',
        'reload_status': '$RELOAD_STATUS',
    }, f, indent=2)
print('[M3-retrain] Event written to $M3_STATE_DIR/last_retrain_event.json')
"

echo "[M3-retrain] $(date) Done"
