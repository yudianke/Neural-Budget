#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INPUT_PATH="${1:-${M1_FEEDBACK_INPUT:-/tmp/m1_ray_feedback.jsonl}}"
OUTPUT_PATH="${2:-${M1_FEEDBACK_DATASET:-/tmp/m1_ray_feedback.csv}}"
CONFIG_PATH="${M1_RAY_CONFIG:-${SCRIPT_DIR}/config_m1_ray.yaml}"

python3 "${SCRIPT_DIR}/build_feedback_dataset.py" \
  --input "${INPUT_PATH}" \
  --output "${OUTPUT_PATH}"

export M1_RAY_PRODUCTION_PATH="${OUTPUT_PATH}"
python3 "${SCRIPT_DIR}/train_m1_ray.py" \
  --mode retrain \
  --config "${CONFIG_PATH}"
