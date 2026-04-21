"""
One-shot export script: fetches the latest M2 model from MLflow, converts to model.onnx.
Run once before building the Docker image or starting the server.

Usage:
    pip install -r requirements_export.txt
    python export_to_onnx.py
"""
import os
import tempfile
import warnings

import mlflow
import mlflow.sklearn
import requests
from mlflow.tracking import MlflowClient
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType

MLFLOW_URL = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.26.214:8000")
REGISTERED_MODEL_NAME = "m2-anomaly"
# Fallback: hardcoded logged-model ID (used only if registry query fails)
FALLBACK_LOGGED_MODEL_ID = "m-c0a5e85dd6b0494ba3b1fa394db99480"
# 6 features in FEATURE_COLS order from train_m2.py:
# abs_amount, repeat_count, is_recurring_candidate,
# user_txn_index, user_mean_abs_amount_prior, user_std_abs_amount_prior
N_FEATURES = 6
OUTPUT_ONNX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model.onnx")

warnings.filterwarnings("ignore", category=FutureWarning)



# def load_latest_from_registry() -> object:
#     """Query MLflow model registry for the latest version and load it."""
#     mlflow.set_tracking_uri(MLFLOW_URL)
#     client = MlflowClient()

#     versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
#     if not versions:
#         raise RuntimeError(f"No versions found for model '{REGISTERED_MODEL_NAME}'")

#     latest = max(versions, key=lambda v: int(v.version))
#     print(f"  Registry: found v{latest.version}  (run_id={latest.run_id})")

#     model_uri = f"runs:/{latest.run_id}/model"
#     print(f"  Loading from {model_uri} ...")
#     pipeline = mlflow.sklearn.load_model(model_uri)
#     return pipeline

def load_latest_from_registry() -> object:
    """Load the latest registered model directly by model name."""
    mlflow.set_tracking_uri(MLFLOW_URL)

    model_uri = "models:/m2-anomaly/latest"
    print(f"  Loading from {model_uri} ...")
    pipeline = mlflow.sklearn.load_model(model_uri)
    return pipeline

def load_from_rest_fallback() -> object:
    """Fallback: download model files via REST API (same pattern as m1_baseline/real_model.py)."""
    dest = tempfile.mkdtemp()
    model_dir = os.path.join(dest, "model")
    os.makedirs(model_dir, exist_ok=True)

    url = f"{MLFLOW_URL.rstrip('/')}/ajax-api/2.0/mlflow/logged-models/{FALLBACK_LOGGED_MODEL_ID}/artifacts/files"
    files = ["MLmodel", "model.pkl", "conda.yaml", "python_env.yaml", "requirements.txt"]
    for fname in files:
        resp = requests.get(url, params={"artifact_file_path": fname}, timeout=30)
        resp.raise_for_status()
        with open(os.path.join(model_dir, fname), "wb") as f:
            f.write(resp.content)
        print(f"  downloaded {fname} ({len(resp.content):,} bytes)")

    pipeline = mlflow.sklearn.load_model(model_dir)
    return pipeline


def main():
    print(f"MLflow server: {MLFLOW_URL}")
    print(f"Registered model: {REGISTERED_MODEL_NAME}")

    # Try registry first (always gets latest), fall back to REST download
    try:
        print("Attempting to load latest version from registry ...")
        pipeline = load_latest_from_registry()
    except Exception as e:
        print(f"  Registry load failed ({e}), falling back to REST API ...")
        pipeline = load_from_rest_fallback()

    print(f"  Pipeline: {pipeline}")

    print("Converting to ONNX ...")
    initial_types = [("float_input", FloatTensorType([None, N_FEATURES]))]
    onnx_model = convert_sklearn(
        pipeline,
        name="m2_anomaly_pipeline",
        initial_types=initial_types,
        target_opset={"": 17, "ai.onnx.ml": 3},
    )

    with open(OUTPUT_ONNX, "wb") as f:
        f.write(onnx_model.SerializeToString())
    size_kb = os.path.getsize(OUTPUT_ONNX) / 1024
    print(f"Wrote {OUTPUT_ONNX}  ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
