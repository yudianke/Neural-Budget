"""
M3 Forecast Training (HistGradientBoostingRegressor)

Usage:
    python train_m3.py

Environment variables:
    MLFLOW_TRACKING_URI       MLflow server URL (default: http://129.114.27.211:8000)
    M3_GATE_MAE               MAE threshold for absolute quality gate (default: 150.0)
    M3_DATA_BUCKET            S3 bucket for training data (default: neural-budget-data-proj16)
    M3_DATA_PREFIX            S3 prefix for forecasting CSVs (default: processed/batch_datasets)
    AWS_ACCESS_KEY_ID         Chameleon object storage key
    AWS_SECRET_ACCESS_KEY     Chameleon object storage secret
    MLFLOW_S3_ENDPOINT_URL    S3 endpoint (default: https://chi.tacc.chameleoncloud.org:7480)

Data source priority:
    1. S3 bucket (if AWS credentials are set)
    2. Local filesystem (fallback for development)

Outputs:
    - MLflow run with metrics, params, artifacts
    - Registers new model version in MLflow registry (m3-forecast) if quality gate passes
    - No local file artifacts — serving loads from MLflow
"""
import io
import os
import sys
import time
import tempfile
from pathlib import Path

import boto3
import joblib
import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.27.211:8000")
MODEL_NAME       = "m3-forecast"
EXPERIMENT_NAME  = "m3-forecast"
MAE_GATE         = float(os.environ.get("M3_GATE_MAE", "150.0"))
S3_BUCKET        = os.environ.get("M3_DATA_BUCKET", "neural-budget-data-proj16")
S3_PREFIX        = os.environ.get("M3_DATA_PREFIX", "processed/batch_datasets")
S3_ENDPOINT      = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "https://chi.tacc.chameleoncloud.org:7480")

# Local fallback paths (used when AWS credentials are not set)
LOCAL_DATA_PATH  = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_train.csv"
LOCAL_EVAL_PATH  = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_eval.csv"


# ---------------------------------------------------------------------------
# Data loading — S3 first, local fallback
# ---------------------------------------------------------------------------
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def _load_csv_from_s3(key: str) -> pd.DataFrame:
    s3 = _s3_client()
    print(f"  Downloading s3://{S3_BUCKET}/{key} ...")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def load_data_csv(filename: str, local_path: Path) -> pd.DataFrame:
    """Load CSV from S3 if credentials available, else local filesystem."""
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    if aws_key:
        try:
            key = f"{S3_PREFIX}/{filename}"
            df = _load_csv_from_s3(key)
            print(f"  Loaded {filename} from S3 ({len(df):,} rows)")
            return df
        except Exception as e:
            print(f"  S3 load failed ({e}), falling back to local...")

    if not local_path.exists():
        raise FileNotFoundError(
            f"No data found. Set AWS_ACCESS_KEY_ID to load from S3, "
            f"or ensure {local_path} exists."
        )
    df = pd.read_csv(local_path)
    print(f"  Loaded {filename} from local ({len(df):,} rows)")
    return df

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
def _add_trig_features(df: pd.DataFrame) -> pd.DataFrame:
    df["month"] = pd.to_datetime(df["year_month"]).dt.month
    df["year"]  = pd.to_datetime(df["year_month"]).dt.year
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    return df


def _build_supervised_rows(monthly_df: pd.DataFrame, min_history: int = 3) -> pd.DataFrame:
    """Build supervised lag-feature rows from a simple monthly aggregation.

    Input columns: synthetic_user_id (optional), year_month, project_category, monthly_spend
    Output: one row per (user, category, month) with lag features and target.
    """
    rows = []
    group_cols = ["synthetic_user_id", "project_category"] if "synthetic_user_id" in monthly_df.columns else ["project_category"]

    for keys, grp in monthly_df.groupby(group_cols):
        grp = grp.sort_values("year_month").reset_index(drop=True)
        history  = grp["monthly_spend"].tolist()
        months   = grp["year_month"].tolist()
        user_id  = keys[0] if "synthetic_user_id" in monthly_df.columns else "single_user"
        cat      = keys[-1]

        if len(history) < min_history + 1:
            continue

        for i in range(min_history, len(history)):
            prior  = history[:i]
            prior3 = prior[-3:]
            prior6 = prior[-6:]
            target = history[i]
            month_num = int(months[i][5:7])
            year      = int(months[i][:4])
            rows.append({
                "synthetic_user_id": user_id,
                "project_category":  cat,
                "year_month":        months[i],
                "target_next_month_spend": target,
                "monthly_spend":     prior[-1],
                "lag_1":  prior[-1] if len(prior) >= 1 else 0,
                "lag_2":  prior[-2] if len(prior) >= 2 else 0,
                "lag_3":  prior[-3] if len(prior) >= 3 else 0,
                "lag_6":  prior[-6] if len(prior) >= 6 else 0,
                "rolling_mean_3": float(np.mean(prior3)),
                "rolling_std_3":  float(np.std(prior3))  if len(prior3) > 1 else 0.0,
                "rolling_mean_6": float(np.mean(prior6)),
                "rolling_max_3":  float(np.max(prior3)),
                "history_month_count": len(prior),
                "month_num": month_num,
                "quarter":   (month_num - 1) // 3 + 1,
                "year":      year,
                "is_q4":     1 if month_num in [10, 11, 12] else 0,
                "month_sin": float(np.sin(2 * np.pi * month_num / 12)),
                "month_cos": float(np.cos(2 * np.pi * month_num / 12)),
            })
    return pd.DataFrame(rows)


def load_data(filename: str, local_path: Path):
    df = load_data_csv(filename, local_path)

    # Detect format:
    # - "supervised" format has target_next_month_spend and lag columns already
    # - "simple" format has only [synthetic_user_id, year_month, project_category, monthly_spend]
    if "target_next_month_spend" not in df.columns:
        print(f"  Detected simple monthly format — building supervised rows...")
        df = _build_supervised_rows(df)
        print(f"  Built {len(df):,} supervised rows")

    if "year_month" in df.columns and "month" not in df.columns:
        df = _add_trig_features(df)

    drop_cols = ["synthetic_user_id", "year_month"] + [
        c for c in DROP_AT_TRAIN if c in df.columns
    ]
    target = "target_next_month_spend"

    X = df.drop(columns=[c for c in drop_cols + [target] if c in df.columns])
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

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    data_source = f"s3://{S3_BUCKET}/{S3_PREFIX}" if aws_key else str(LOCAL_DATA_PATH.parent)
    print(f"Loading training data from: {data_source}")

    X_train, y_train, train_df = load_data("forecasting_train.csv", LOCAL_DATA_PATH)
    X_eval, y_eval, eval_df   = load_data("forecasting_eval.csv",  LOCAL_EVAL_PATH)

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
        mlflow.log_param("data_source", data_source)

        mlflow.log_metric("overall_mae", overall_mae)
        mlflow.log_metric("median_per_category_mae", median_per_cat_mae)
        mlflow.log_metric("train_time_seconds", train_time)

        for _, row in per_cat.iterrows():
            name = row["project_category"].replace(" ", "_").replace("/", "_")
            mlflow.log_metric(f"mae_{name}", float(row["mae"]))

        # Log bundle as MLflow artifact (serving loads from here, not local disk)
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            bundle_path = Path(tmpdir) / "m3_bundle.joblib"
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
