import argparse
import os
import platform
import subprocess
import time
import warnings
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import yaml
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
warnings.filterwarnings("ignore")

FEATURE_COLS = [
    "abs_amount",
    "repeat_count",
    "is_recurring_candidate",
    "user_txn_index",
    "user_mean_abs_amount_prior",
    "user_std_abs_amount_prior",
]
USER_COL = "synthetic_user_id"

CACHE_DIR = Path(os.environ.get("DATA_CACHE_DIR", "/tmp/nb_data_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def log(msg):
    print(f"[M2 {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def _resolve_path(path):
    if not path.startswith("swift://"):
        return path
    _, rest = path.split("swift://", 1)
    container, object_name = rest.split("/", 1)
    local_path = CACHE_DIR / container / object_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if not local_path.exists():
        log(f"downloading {path} -> {local_path}")
        subprocess.run(["swift", "download", container, object_name, "-o", str(local_path)], check=True)
    else:
        log(f"using cached {local_path}")
    return str(local_path)


def load_split(path, sample_frac=None, max_rows=None):
    local = _resolve_path(path)
    log(f"reading csv {local}")
    df = pd.read_csv(local)
    log(f"loaded {len(df):,} rows, {df[USER_COL].nunique()} users")

    if max_rows is not None and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)
        log(f"sampled down to {len(df):,} rows for memory")
    elif sample_frac is not None and sample_frac < 1.0:
        df = df.sample(frac=sample_frac, random_state=42).reset_index(drop=True)
        log(f"sampled down to {len(df):,} rows ({sample_frac:.0%})")

    df = df.dropna(subset=[USER_COL, "abs_amount"])
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def seed_anomalies(df, sigma):
    """Inject synthetic anomalies at user_mean + sigma * user_std."""
    log(f"seeding anomalies at {sigma}*std per user")
    seeded_rows = []
    user_groups = df.groupby(USER_COL)
    n_users = len(user_groups)
    for i, (_, group) in enumerate(user_groups):
        mean = group["user_mean_abs_amount_prior"].iloc[-1]
        std = group["user_std_abs_amount_prior"].iloc[-1]
        if not np.isfinite(std) or std <= 0:
            std = max(mean * 0.1, 1.0)
        n_inject = max(2, len(group) // 30)
        sample = group.sample(n=min(n_inject, len(group)), replace=True, random_state=42).copy()
        sample["abs_amount"] = mean + sigma * std
        seeded_rows.append(sample)
        if (i + 1) % 1000 == 0:
            log(f"  seeded {i + 1}/{n_users} users")
    seeded = pd.concat(seeded_rows, ignore_index=True)
    log(f"seeded {len(seeded):,} synthetic anomaly rows")
    return seeded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m2.yaml")
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", config.get("mlflow_tracking_uri"))
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(config["experiment_name"])

    train_path = os.environ.get("M2_TRAIN_PATH", config["train_path"])
    eval_path = os.environ.get("M2_EVAL_PATH", config["eval_path"])
    gate_recall = float(os.environ.get("M2_GATE_RECALL", config.get("quality_gate_recall", 0.70)))
    sigma = float(os.environ.get("M2_ANOMALY_SIGMA", config.get("anomaly_sigma", 5.0)))
    max_train_rows = int(os.environ.get("M2_MAX_TRAIN_ROWS", config.get("max_train_rows", 200000)))
    max_eval_rows = int(os.environ.get("M2_MAX_EVAL_ROWS", config.get("max_eval_rows", 50000)))
    registered_model_name = config.get("registered_model_name", "m2-anomaly-base")

    log(f"tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"eval_path={eval_path}")
    log(f"gate_recall={gate_recall} sigma={sigma}")
    log(f"max_train_rows={max_train_rows} max_eval_rows={max_eval_rows}")

    log("=== loading train ===")
    train_df = load_split(train_path, max_rows=max_train_rows)

    log("=== loading eval ===")
    eval_df = load_split(eval_path, max_rows=max_eval_rows)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "model_type": "isolation_forest_global_base",
                "contamination": config["isolation_forest"]["contamination"],
                "n_estimators": config["isolation_forest"]["n_estimators"],
                "random_state": config["isolation_forest"]["random_state"],
                "train_rows": len(train_df),
                "eval_rows": len(eval_df),
                "train_users": train_df[USER_COL].nunique(),
                "eval_users": eval_df[USER_COL].nunique(),
                "feature_cols": ",".join(FEATURE_COLS),
                "gate_recall": gate_recall,
                "anomaly_sigma": sigma,
                "max_train_rows": max_train_rows,
                "max_eval_rows": max_eval_rows,
                "train_path": train_path,
                "eval_path": eval_path,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
        )

        log("=== fitting scaler ===")
        scaler = StandardScaler()
        X_train = scaler.fit_transform(train_df[FEATURE_COLS].values.astype(float))
        log(f"scaled train shape: {X_train.shape}")

        log("=== training base IsolationForest ===")
        start = time.time()
        clf = IsolationForest(
            contamination=config["isolation_forest"]["contamination"],
            n_estimators=config["isolation_forest"]["n_estimators"],
            random_state=config["isolation_forest"]["random_state"],
            n_jobs=-1,
        )
        clf.fit(X_train)
        train_time = time.time() - start
        log(f"trained in {train_time:.1f}s")
        mlflow.log_metric("train_time_seconds", train_time)

        log("=== scoring real eval ===")
        X_eval = scaler.transform(eval_df[FEATURE_COLS].values.astype(float))
        real_preds = clf.predict(X_eval)
        real_flag_rate = (real_preds == -1).mean()
        log(f"real_flag_rate={real_flag_rate:.4f} on {len(eval_df):,} rows")

        log("=== building seeded anomalies ===")
        seeded_df = seed_anomalies(eval_df, sigma)
        X_seeded = scaler.transform(seeded_df[FEATURE_COLS].values.astype(float))

        log("=== scoring seeded anomalies ===")
        seeded_preds = clf.predict(X_seeded)
        n_caught = int((seeded_preds == -1).sum())
        n_seeded = len(seeded_df)
        recall = n_caught / n_seeded if n_seeded > 0 else 0.0
        log(f"seeded_recall={recall:.4f} ({n_caught}/{n_seeded})")

        mlflow.log_metric("real_flag_rate", real_flag_rate)
        mlflow.log_metric("seeded_anomaly_count", n_seeded)
        mlflow.log_metric("seeded_anomaly_caught", n_caught)
        mlflow.log_metric("seeded_anomaly_recall", recall)

        pipeline = Pipeline([("scaler", scaler), ("iforest", clf)])

        passed = recall >= gate_recall
        mlflow.set_tag("quality_gate_metric", "seeded_anomaly_recall")
        mlflow.set_tag("quality_gate_passed", str(passed).lower())
        mlflow.set_tag("quality_gate_threshold", str(gate_recall))
        mlflow.set_tag("model_role", "cold_start_base")

        if passed:
            mlflow.sklearn.log_model(
                pipeline,
                artifact_path="model",
                registered_model_name=registered_model_name,
            )
            log(f"PASSED gate (recall={recall:.4f} >= {gate_recall}) — registered as '{registered_model_name}'")
        else:
            mlflow.set_tag("rejected_by_gate", "true")
            mlflow.sklearn.log_model(pipeline, artifact_path="model")
            log(f"FAILED gate (recall={recall:.4f} < {gate_recall}) — NOT registered")

        log(f"done | run_id={run.info.run_id}")


if __name__ == "__main__":
    main()
