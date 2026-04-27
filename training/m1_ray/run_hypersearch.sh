#!/usr/bin/env bash
# run_hypersearch.sh — Sequential hyperparameter search for M1 XGBoost (sparse)
# Runs all 4 configs in order on the full dataset (1.35M train / 339k eval).
# Best model passing the quality gate is auto-registered to MLflow.
#
# Usage (local):
#   bash training/m1_ray/run_hypersearch.sh
#
# Usage (inside retrain-daemon container on VM):
#   docker exec -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
#     c6ade148e792_retrain-daemon bash /app/training/m1_ray/run_hypersearch.sh
#
# Monitor: MLflow UI http://129.114.25.192:8000/#/experiments/4
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/hypersearch_logs"
mkdir -p "${LOG_DIR}"

export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-http://129.114.25.192:8000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-0fc136376b7c47528dfd06a09d12ccbd}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-b52fcbb61618453aacc6ababb530031c}"
export MLFLOW_S3_ENDPOINT_URL="${MLFLOW_S3_ENDPOINT_URL:-https://chi.tacc.chameleoncloud.org:7480}"
export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-https://chi.tacc.chameleoncloud.org:7480}"

CONFIGS=(
    "config_m1_ray.yaml"
    "config_m1_ray_v2.yaml"
    "config_m1_ray_v3.yaml"
    "config_m1_ray_v4.yaml"
)

LABELS=(
    "baseline  (n=200 depth=6  lr=0.10 ngram=1-3 feat=1000)"
    "v2        (n=300 depth=8  lr=0.05 ngram=1-3 feat=2000)"
    "v3        (n=200 depth=4  lr=0.15 ngram=1-4 feat=1500)"
    "v4        (n=400 depth=6  lr=0.05 ngram=2-4 feat=2000)"
)

echo "========================================"
echo "M1 Hyperparameter Search (full dataset)"
echo "Started: $(date)"
echo "Configs: ${#CONFIGS[@]}"
echo "MLflow:  ${MLFLOW_TRACKING_URI}"
echo "========================================"

RESULTS=()

for i in "${!CONFIGS[@]}"; do
    CFG="${CONFIGS[$i]}"
    LABEL="${LABELS[$i]}"
    LOG="${LOG_DIR}/run_${i}_${CFG%.yaml}.log"

    echo ""
    echo "----------------------------------------"
    echo "Run $((i+1))/${#CONFIGS[@]}: ${LABEL}"
    echo "Config: ${CFG}"
    echo "Log:    ${LOG}"
    echo "Start:  $(date)"
    echo "----------------------------------------"

    START_TS=$(date +%s)

    if python3 "${SCRIPT_DIR}/train_m1_ray.py" \
        --mode bootstrap \
        --config "${SCRIPT_DIR}/${CFG}" \
        2>&1 | tee "${LOG}"; then
        STATUS="OK"
    else
        STATUS="FAILED"
    fi

    END_TS=$(date +%s)
    ELAPSED=$(( END_TS - START_TS ))
    ELAPSED_MIN=$(( ELAPSED / 60 ))

    MACRO_F1=$(grep -oE 'macro_f1=[0-9.]+' "${LOG}" | tail -1 | cut -d= -f2 || echo "?")
    REGISTERED=$(grep -oE 'REGISTERED v[0-9]+' "${LOG}" | tail -1 | grep -oE '[0-9]+' || echo "no")

    RESULT="${LABEL} | macro_f1=${MACRO_F1} | registered=v${REGISTERED} | time=${ELAPSED_MIN}m | status=${STATUS}"
    RESULTS+=("${RESULT}")

    echo ""
    echo "Finished run $((i+1)): ${RESULT}"
done

echo ""
echo "========================================"
echo "HYPERSEARCH COMPLETE: $(date)"
echo "========================================"
echo ""
echo "Results summary:"
for i in "${!RESULTS[@]}"; do
    echo "  $((i+1)). ${RESULTS[$i]}"
done
echo ""
echo "MLflow UI: ${MLFLOW_TRACKING_URI}/#/experiments/4"
echo "Logs:      ${LOG_DIR}/"
