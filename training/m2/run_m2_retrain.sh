#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INPUT_PATH="${1:-${M2_FEEDBACK_INPUT:-/data/feedback/m2_feedback.jsonl}}"
OUTPUT_PATH="${2:-${M2_FEEDBACK_DATASET:-/data/feedback/m2_feedback_dataset.csv}}"
CONFIG_PATH="${M2_CONFIG:-${SCRIPT_DIR}/config_m2.yaml}"

# M2_TRAIN_PATH: explicit override for bootstrap training data path.
# Set by the retrain daemon from M2_BOOTSTRAP_DATA_PATH env var.
# Overrides config_m2.yaml train_path (e.g. swift:// on Chameleon).
# If unset, train_m2.py falls back to train_path from the config YAML.
export M2_TRAIN_PATH="${M2_TRAIN_PATH:-}"

echo "[M2-RETRAIN] Building feedback dataset..."
python3 "${SCRIPT_DIR}/build_feedback_dataset.py" \
  --input "${INPUT_PATH}" \
  --output "${OUTPUT_PATH}"

echo "[M2-RETRAIN] Running retrain with feedback..."
export M2_PRODUCTION_PATH="${OUTPUT_PATH}"
python3 "${SCRIPT_DIR}/train_m2.py" \
  --mode retrain \
  --config "${CONFIG_PATH}"
