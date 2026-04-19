"""
M3 Forecast Training — v2 (HistGradientBoostingRegressor)

Usage:
    python train_m3_v2.py

Environment variables:
    MLFLOW_TRACKING_URI   MLflow server URL (default: http://129.114.27.211:8000)
    M3_GATE_MAE           MAE threshold for absolute quality gate (default: 150.0)

Outputs:
    - MLflow run with metrics, params, artifacts
    - Registers new model version in MLflow registry (m3-forecast-v2) if quality gate passes
    - No local file artifacts — serving loads from MLflow
"""
import os
import sys
import time
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_PATH = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_v2_train.csv"
EVAL_PATH = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_v2_eval.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.27.211:8000")
MODEL_NAME       = "m3-forecast-v2"
EXPERIMENT_NAME  = "m3-forecast-v2"
MAE_GATE         = float(os.environ.get("M3_GATE_MAE", "150.0"))

# Features that cannot be computed from real ActualBudget data at inference time.
# Drop them so the served model only uses features the inference service can provide.
DROP_AT_TRAIN = [
    "persona_cluster",
    "AGE_REF",
    "FAM_SIZE",
    "user_scale",
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_data(path: Path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["year_month"]).dt.month
    df["year"] = pd.to_datetime(df["year_month"]).dt.year

    # Trig encoding of month — computable at inference time
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    drop_cols = ["synthetic_user_id", "year_month"] + [
        c for c in DROP_AT_TRAIN if c in df.columns
    ]
    target = "target_next_month_spend"

    X = df.drop(columns=drop_cols + [target])
    y = df[target]
    return X, y, df


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------
def evaluate_per_category(df, y_true, y_pred):
    tmp = df.copy()
    tmp["y_true"] = y_true
    tmp["y_pred"] = y_pred
    tmp["abs_err"] = (tmp["y_true"] - tmp["y_pred"]).abs()
    return tmp.groupby("project_category")["abs_err"].mean().reset_index(name="mae")


def get_previous_mae(client: MlflowClient) -> float | None:
    """Return the overall_mae of the latest registered version, or None."""
    try:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not versions:
            return None
        latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        run = client.get_run(latest.run_id)
        return run.data.metrics.get("overall_mae")
    except Exception as e:
        print(f"[WARN] could not fetch previous MAE: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient(tracking_uri=MLFLOW_URI)

    print(f"Loading training data from {DATA_PATH}")
    X_train, y_train, train_df = load_data(DATA_PATH)
    X_eval, y_eval, eval_df = load_data(EVAL_PATH)

    # Consistent one-hot encoding across train/eval
    X_train = pd.get_dummies(X_train, dummy_na=True)
    X_eval = pd.get_dummies(X_eval, dummy_na=True)
    X_eval = X_eval.reindex(columns=X_train.columns, fill_value=0)

    feature_columns = list(X_train.columns)
    print(f"Training with {len(feature_columns)} features, {len(train_df)} rows")

    with mlflow.start_run() as run:
        start = time.time()

        model = HistGradientBoostingRegressor(
            max_depth=6,
            learning_rate=0.05,
            max_iter=300,
            random_state=42,
        )
        model.fit(X_train, y_train)
        train_time = time.time() - start

        y_pred = model.predict(X_eval)
        overall_mae = mean_absolute_error(y_eval, y_pred)
        per_cat = evaluate_per_category(eval_df, y_eval, y_pred)
        median_per_cat_mae = float(per_cat["mae"].median())

        # ----------------------------------------------------------------
        # MLflow logging
        # ----------------------------------------------------------------
        mlflow.log_param("model", "HistGradientBoostingRegressor")
        mlflow.log_param("features", len(feature_columns))
        mlflow.log_param("train_rows", len(train_df))
        mlflow.log_param("eval_rows", len(eval_df))
        mlflow.log_param("mae_gate", MAE_GATE)
        mlflow.log_param("dropped_at_train", ",".join(DROP_AT_TRAIN))

        mlflow.log_metric("overall_mae", overall_mae)
        mlflow.log_metric("median_per_category_mae", median_per_cat_mae)
        mlflow.log_metric("train_time_seconds", train_time)

        for _, row in per_cat.iterrows():
            name = row["project_category"].replace(" ", "_").replace("/", "_")
            mlflow.log_metric(f"mae_{name}", float(row["mae"]))

        # Log bundle as MLflow artifact (serving loads from here, not local disk)
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_path = Path(tmpdir) / "m3_v2_bundle.joblib"
            bundle = {
                "model": model,
                "feature_columns": feature_columns,
                "model_name": MODEL_NAME,
            }
            joblib.dump(bundle, bundle_path)
            mlflow.log_artifact(str(bundle_path), artifact_path="bundle")

        print(f"Overall MAE:           {overall_mae:.4f}")
        print(f"Median per-cat MAE:    {median_per_cat_mae:.4f}")
        print(f"MAE gate threshold:    {MAE_GATE:.4f}")

        # ----------------------------------------------------------------
        # Quality gate — absolute MAE floor
        # ----------------------------------------------------------------
        absolute_gate_passed = overall_mae <= MAE_GATE

        # Improvement gate — must beat previous registered version
        prev_mae = get_previous_mae(client)
        if prev_mae is None:
            improvement_gate_passed = True
            register_reason = f"first version (mae={overall_mae:.4f})"
        elif overall_mae < prev_mae:
            improvement_gate_passed = True
            register_reason = f"improved mae {prev_mae:.4f} → {overall_mae:.4f}"
        else:
            improvement_gate_passed = False
            register_reason = f"no improvement (prev={prev_mae:.4f}, curr={overall_mae:.4f})"

        do_register = absolute_gate_passed and improvement_gate_passed

        mlflow.set_tag("absolute_gate_passed", str(absolute_gate_passed).lower())
        mlflow.set_tag("improvement_gate_passed", str(improvement_gate_passed).lower())
        mlflow.set_tag("registered", str(do_register).lower())
        mlflow.set_tag("register_reason", register_reason)
        mlflow.set_tag("previous_mae", str(prev_mae))

        if not absolute_gate_passed:
            print(f"GATE FAILED: mae={overall_mae:.4f} > threshold={MAE_GATE:.4f} — not registering")
            sys.exit(0)

        if not improvement_gate_passed:
            print(f"GATE FAILED: {register_reason} — not registering")
            sys.exit(0)

        # ----------------------------------------------------------------
        # Register model version in MLflow
        # ----------------------------------------------------------------
        try:
            client.get_registered_model(MODEL_NAME)
        except Exception:
            client.create_registered_model(MODEL_NAME)

        model_uri = f"runs:/{run.info.run_id}/bundle"
        mv = client.create_model_version(
            name=MODEL_NAME,
            source=model_uri,
            run_id=run.info.run_id,
        )
        print(f"Registered {MODEL_NAME} v{mv.version}: {register_reason}")


if __name__ == "__main__":
    main()
