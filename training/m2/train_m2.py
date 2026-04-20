import argparse
import os
import platform
import sys
import time
import warnings

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _common import (  # noqa: E402
    get_latest_production_metric,
    load_and_combine,
    load_config,
    log as _log,
    register_model_version,
    setup_mlflow,
    should_register,
)

warnings.filterwarnings("ignore")


def log(msg):
    _log("M2", msg)


FEATURE_COLS = [
    "abs_amount",
    "repeat_count",
    "is_recurring_candidate",
    "user_txn_index",
    "user_mean_abs_amount_prior",
    "user_std_abs_amount_prior",
]
USER_COL = "synthetic_user_id"


def load_split(bootstrap_path, production_path=None, max_rows=None):
    df = load_and_combine(bootstrap_path, production_path, prefix="M2")
    if max_rows is not None and len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)
        log(f"sampled to {len(df):,} rows for memory")
    df = df.dropna(subset=[USER_COL, "abs_amount"])
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def seed_anomalies(df, sigma):
    log(f"seeding anomalies at {sigma}*std")
    seeded = []
    for _, group in df.groupby(USER_COL):
        mean = group["user_mean_abs_amount_prior"].iloc[-1]
        std = group["user_std_abs_amount_prior"].iloc[-1]
        if not np.isfinite(std) or std <= 0:
            std = max(mean * 0.1, 1.0)
        n_rows = max(2, len(group) // 30)
        sample = group.sample(n=min(n_rows, len(group)), replace=True, random_state=42).copy()
        sample["abs_amount"] = mean + sigma * std
        seeded.append(sample)
    out = pd.concat(seeded, ignore_index=True)
    log(f"seeded {len(out):,} anomaly rows")
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m2.yaml")
    parser.add_argument("--mode", choices=["bootstrap", "retrain"], default="bootstrap")
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = setup_mlflow(config)
    train_path = os.environ.get("M2_TRAIN_PATH") or config["train_path"]
    eval_path = os.environ.get("M2_EVAL_PATH", config["eval_path"])
    production_path = config.get("production_path") if args.mode == "retrain" else None
    gate_recall = float(os.environ.get("M2_GATE_RECALL", config.get("quality_gate_recall", 0.70)))
    sigma = float(os.environ.get("M2_ANOMALY_SIGMA", config.get("anomaly_sigma", 5.0)))
    max_train_rows = int(os.environ.get("M2_MAX_TRAIN_ROWS", config.get("max_train_rows", 200000)))
    max_eval_rows = int(os.environ.get("M2_MAX_EVAL_ROWS", config.get("max_eval_rows", 50000)))
    registered_model_name = config.get("registered_model_name", "m2-anomaly")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"production_path={production_path}")
    log(f"gate_recall={gate_recall} sigma={sigma}")

    train_df = load_split(train_path, production_path, max_rows=max_train_rows)
    eval_df = load_split(eval_path, None, max_rows=max_eval_rows)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "mode": args.mode,
                "model_type": "isolation_forest_global_base",
                "contamination": config["isolation_forest"]["contamination"],
                "n_estimators": config["isolation_forest"]["n_estimators"],
                "random_state": config["isolation_forest"]["random_state"],
                "train_rows": len(train_df),
                "eval_rows": len(eval_df),
                "gate_recall": gate_recall,
                "anomaly_sigma": sigma,
                "train_path": train_path,
                "production_path": production_path or "",
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
        )

        log("=== fitting scaler + IF ===")
        scaler = StandardScaler()
        X_train = scaler.fit_transform(train_df[FEATURE_COLS].values.astype(float))

        start = time.time()
        clf = IsolationForest(
            contamination=config["isolation_forest"]["contamination"],
            n_estimators=config["isolation_forest"]["n_estimators"],
            random_state=config["isolation_forest"]["random_state"],
            n_jobs=-1,
        )
        clf.fit(X_train)
        train_time = time.time() - start
        mlflow.log_metric("train_time_seconds", train_time)
        log(f"trained in {train_time:.1f}s")

        X_eval = scaler.transform(eval_df[FEATURE_COLS].values.astype(float))
        real_preds = clf.predict(X_eval)
        real_flag_rate = float((real_preds == -1).mean())

        seeded_df = seed_anomalies(eval_df, sigma)
        X_seeded = scaler.transform(seeded_df[FEATURE_COLS].values.astype(float))
        seeded_preds = clf.predict(X_seeded)
        n_caught = int((seeded_preds == -1).sum())
        n_seeded = len(seeded_df)
        recall = n_caught / n_seeded if n_seeded > 0 else 0.0

        log(f"real_flag_rate={real_flag_rate:.4f}")
        log(f"seeded_recall={recall:.4f} ({n_caught}/{n_seeded})")

        mlflow.log_metric("real_flag_rate", real_flag_rate)
        mlflow.log_metric("seeded_anomaly_count", n_seeded)
        mlflow.log_metric("seeded_anomaly_caught", n_caught)
        mlflow.log_metric("seeded_anomaly_recall", recall)

        pipeline = Pipeline([("scaler", scaler), ("iforest", clf)])

        absolute_passed = recall >= gate_recall
        prev_version, prev_metric = get_latest_production_metric(registered_model_name, "seeded_anomaly_recall")
        log(f"previous: v{prev_version} recall={prev_metric}")

        do_register, reason = should_register(
            mode=args.mode,
            current_metric=recall,
            previous_metric=prev_metric,
            higher_is_better=True,
            absolute_gate_passed=absolute_passed,
            prefix="M2",
        )

        mlflow.set_tag("quality_gate_metric", "seeded_anomaly_recall")
        mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
        mlflow.set_tag("registered", str(do_register).lower())
        mlflow.set_tag("register_reason", reason)
        mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
        mlflow.set_tag("mode", args.mode)
        mlflow.set_tag("model_role", "cold_start_base")

        if do_register:
            mlflow.sklearn.log_model(pipeline, artifact_path="model")
            client = MlflowClient()
            mv = register_model_version(client, registered_model_name, run.info.run_id, "model")
            log(f"REGISTERED v{mv.version} — {reason}")
        else:
            mlflow.sklearn.log_model(pipeline, artifact_path="model")
            log(f"NOT REGISTERED — {reason}")

        log(f"done | run_id={run.info.run_id}")


if __name__ == "__main__":
    main()
