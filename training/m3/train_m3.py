import argparse
import os
import platform
import sys
import tempfile
import time
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient

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


def log(msg):
    _log("M3", msg)


USER_COL = "synthetic_user_id"
DATE_COL = "year_month"
CATEGORY_COL = "project_category"
SPEND_COL = "monthly_spend"


def load_split(bootstrap_path, production_path=None):
    df = load_and_combine(bootstrap_path, production_path, prefix="M3")
    log(f"loaded {len(df):,} rows, {df[USER_COL].nunique()} users, {df[CATEGORY_COL].nunique()} categories")
    df = df.dropna(subset=[CATEGORY_COL, SPEND_COL])
    return df


def build_baseline_table(train_df):
    log("computing per-category population statistics")
    grouped = train_df.groupby(CATEGORY_COL)
    stats = grouped[SPEND_COL].agg(
        mean="mean",
        std="std",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        n_observations="count",
    ).reset_index()
    n_users = grouped[USER_COL].nunique().reset_index()
    n_users.columns = [CATEGORY_COL, "n_users"]
    stats = stats.merge(n_users, on=CATEGORY_COL)
    stats["std"] = stats["std"].fillna(0.0)
    return stats


def evaluate_baseline(eval_df, baseline_table):
    means = baseline_table.set_index(CATEGORY_COL)["mean"].to_dict()
    scored = eval_df.copy()
    scored["pred_mean"] = scored[CATEGORY_COL].map(means)
    scored = scored.dropna(subset=["pred_mean"])
    scored["abs_err"] = (scored["pred_mean"] - scored[SPEND_COL]).abs()
    per_cat = scored.groupby(CATEGORY_COL).agg(mae=("abs_err", "mean"), n=("abs_err", "count")).reset_index()
    overall_mean_mae = float(scored["abs_err"].mean())
    median_per_cat_mae = float(per_cat["mae"].median())
    return per_cat, overall_mean_mae, median_per_cat_mae


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m3.yaml")
    parser.add_argument("--mode", choices=["bootstrap", "retrain"], default="bootstrap")
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = setup_mlflow(config)
    train_path = os.environ.get("M3_TRAIN_PATH", config["train_path"])
    eval_path = os.environ.get("M3_EVAL_PATH", config["eval_path"])
    production_path = config.get("production_path") if args.mode == "retrain" else None
    gate_median_mae = float(
        os.environ.get("M3_GATE_MEDIAN_MAE", config.get("quality_gate_median_mae", 1000.0))
    )
    registered_model_name = config.get("registered_model_name", "m3-forecast-base")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"production_path={production_path}")
    log(f"gate_median_mae={gate_median_mae}")

    train_df = load_split(train_path, production_path)
    eval_df = load_split(eval_path, None)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "mode": args.mode,
                "model_type": "population_baseline_per_category",
                "model_role": "cold_start_base",
                "train_path": train_path,
                "production_path": production_path or "",
                "eval_path": eval_path,
                "train_rows": len(train_df),
                "eval_rows": len(eval_df),
                "gate_median_mae": gate_median_mae,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
        )

        start = time.time()
        baseline_table = build_baseline_table(train_df)
        per_cat, overall_mean_mae, median_per_cat_mae = evaluate_baseline(eval_df, baseline_table)
        train_time = time.time() - start

        log(f"baseline computed in {train_time:.2f}s")
        log(f"overall mean MAE = {overall_mean_mae:.2f}")
        log(f"median per-category MAE = {median_per_cat_mae:.2f}")
        log("per-category MAE:")
        for _, row in per_cat.iterrows():
            log(f"  {row[CATEGORY_COL]:20s} mae={row['mae']:>10.2f} n={int(row['n'])}")

        mlflow.log_metric("train_time_seconds", train_time)
        mlflow.log_metric("overall_mean_mae", overall_mean_mae)
        mlflow.log_metric("median_of_per_category_mae", median_per_cat_mae)
        mlflow.log_metric("n_categories", len(baseline_table))
        for _, row in per_cat.iterrows():
            safe = row[CATEGORY_COL].replace(" ", "_").replace("/", "_")
            mlflow.log_metric(f"mae_{safe}", float(row["mae"]))

        with tempfile.TemporaryDirectory() as tmpdir:
            tp = Path(tmpdir) / "m3_baseline.parquet"
            baseline_table.to_parquet(tp, index=False)
            mlflow.log_artifact(str(tp), artifact_path="baseline")
            cp = Path(tmpdir) / "m3_baseline.csv"
            baseline_table.to_csv(cp, index=False)
            mlflow.log_artifact(str(cp), artifact_path="baseline")

        absolute_passed = median_per_cat_mae <= gate_median_mae
        prev_version, prev_metric = get_latest_production_metric(
            registered_model_name, "median_of_per_category_mae"
        )
        log(f"previous: v{prev_version} median_per_cat_mae={prev_metric}")

        do_register, reason = should_register(
            mode=args.mode,
            current_metric=median_per_cat_mae,
            previous_metric=prev_metric,
            higher_is_better=False,
            absolute_gate_passed=absolute_passed,
            prefix="M3",
        )

        mlflow.set_tag("quality_gate_metric", "median_of_per_category_mae")
        mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
        mlflow.set_tag("registered", str(do_register).lower())
        mlflow.set_tag("register_reason", reason)
        mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
        mlflow.set_tag("mode", args.mode)
        mlflow.set_tag("model_role", "cold_start_base")

        if do_register:
            client = MlflowClient()
            mv = register_model_version(client, registered_model_name, run.info.run_id, "baseline")
            log(f"REGISTERED v{mv.version} — {reason}")
        else:
            log(f"NOT REGISTERED — {reason}")

        log(f"done | run_id={run.info.run_id}")


if __name__ == "__main__":
    main()
