import argparse
import json
import os
import platform
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import joblib
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import ray
import xgboost as xgb
from mlflow.tracking import MlflowClient
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.xgboost import XGBoostTrainer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.preprocessing import LabelEncoder
from scipy.sparse import csr_matrix, hstack

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _common import (  # noqa: E402
    get_latest_production_metric,
    load_config,
    log as _log,
    register_model_version,
    resolve_path,
    setup_mlflow,
    should_register,
)
from safeguarding import run_safeguarding_checks  # noqa: E402

os.environ["GIT_PYTHON_REFRESH"] = "quiet"


def log(msg):
    _log("M1-RAY", msg)


TEXT_COL = "merchant"
LABEL_COL = "category"
NUMERIC_COLS = ["log_amount", "day_of_week", "day_of_month"]
CANONICAL_COLUMNS = ["date", TEXT_COL, "amount", LABEL_COL]


def normalize_merchant(name):
    if not isinstance(name, str):
        return ""
    value = name.upper().strip()
    value = re.sub(r"\b\d{4,}\b", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _normalize_bootstrap_df(path):
    local_path = resolve_path(path, "M1-RAY")
    df = pd.read_csv(local_path)
    df = df.rename(
        columns={
            "Transaction Date": "date",
            "Transaction Type": "transaction_type",
            "Transaction Description": TEXT_COL,
            "Debit Amount": "debit_amount",
            "Credit Amount": "credit_amount",
            "Balance": "balance",
            "Category": LABEL_COL,
        }
    )
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["amount"] = df["debit_amount"].fillna(0) - df["credit_amount"].fillna(0)
    df[LABEL_COL] = df[LABEL_COL].astype(str).str.strip()
    df[TEXT_COL] = df[TEXT_COL].astype(str).str.strip()
    df = df.dropna(subset=["date", LABEL_COL, TEXT_COL])
    df = df[df[LABEL_COL] != ""]
    df = df[df[TEXT_COL] != ""]
    df = df[CANONICAL_COLUMNS].copy()
    df["source"] = "bootstrap"
    return df.sort_values("date").reset_index(drop=True), local_path


def _normalize_feedback_df(path):
    local_path = resolve_path(path, "M1-RAY")
    if local_path.endswith(".jsonl"):
        rows = []
        with open(local_path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        df = pd.DataFrame(rows)
    else:
        df = pd.read_csv(local_path)

    rename_map = {
        "chosen_category": LABEL_COL,
        "merchant_clean": TEXT_COL,
        "transaction_description": TEXT_COL,
    }
    df = df.rename(columns=rename_map)
    if TEXT_COL not in df.columns:
        merchant_series = None
        for candidate in ["merchant", "payee_name", "imported_payee"]:
            if candidate in df.columns:
                candidate_series = df[candidate].astype(str).str.strip()
                merchant_series = (
                    candidate_series
                    if merchant_series is None
                    else merchant_series.mask(merchant_series == "", candidate_series)
                )
        df[TEXT_COL] = merchant_series if merchant_series is not None else ""

    if LABEL_COL not in df.columns and "category" in df.columns:
        df[LABEL_COL] = df["category"]

    if "amount" not in df.columns:
        raise ValueError(f"production feedback dataset missing amount column: {local_path}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df[TEXT_COL] = df[TEXT_COL].astype(str).str.strip()
    df[LABEL_COL] = df[LABEL_COL].astype(str).str.strip()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["date", "amount", LABEL_COL, TEXT_COL])
    df = df[df[LABEL_COL] != ""]
    df = df[df[TEXT_COL] != ""]
    df = df[CANONICAL_COLUMNS].copy()
    df["source"] = "production_feedback"
    return df.sort_values("date").reset_index(drop=True), local_path


def _featurize_split(train_df, test_df, config):
    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df["log_amount"] = np.log1p(train_df["amount"].abs())
    test_df["log_amount"] = np.log1p(test_df["amount"].abs())
    train_df["day_of_week"] = train_df["date"].dt.dayofweek
    test_df["day_of_week"] = test_df["date"].dt.dayofweek
    train_df["day_of_month"] = train_df["date"].dt.day
    test_df["day_of_month"] = test_df["date"].dt.day
    train_df["merchant_clean"] = train_df[TEXT_COL].apply(normalize_merchant)
    test_df["merchant_clean"] = test_df[TEXT_COL].apply(normalize_merchant)

    known_cats = set(train_df[LABEL_COL].unique())
    test_df = test_df[test_df[LABEL_COL].isin(known_cats)].copy()

    le = LabelEncoder()
    train_df["label"] = le.fit_transform(train_df[LABEL_COL])
    test_df["label"] = le.transform(test_df[LABEL_COL])

    tfidf = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(config["tfidf"]["ngram_min"], config["tfidf"]["ngram_max"]),
        max_features=config["tfidf"]["max_features"],
    )
    x_train_text = tfidf.fit_transform(train_df["merchant_clean"])
    x_test_text = tfidf.transform(test_df["merchant_clean"])

    x_train_num = train_df[NUMERIC_COLS].fillna(0).values
    x_test_num = test_df[NUMERIC_COLS].fillna(0).values

    x_train = hstack([x_train_text, csr_matrix(x_train_num)]).toarray()
    x_test = hstack([x_test_text, csr_matrix(x_test_num)]).toarray()

    feature_cols = [f"f{i}" for i in range(x_train.shape[1])]
    train_pd = pd.DataFrame(x_train, columns=feature_cols)
    train_pd["label"] = train_df["label"].values
    test_pd = pd.DataFrame(x_test, columns=feature_cols)
    test_pd["label"] = test_df["label"].values

    return train_pd, test_pd, le, tfidf, feature_cols


def load_and_prepare(config, mode):
    bootstrap_df, bootstrap_path = _normalize_bootstrap_df(config["data_path"])
    split_date = pd.to_datetime(config["split_date"])
    train_df = bootstrap_df[bootstrap_df["date"] < split_date].copy()
    test_df = bootstrap_df[bootstrap_df["date"] >= split_date].copy()

    production_path = None
    production_rows = 0
    if mode == "retrain" and config.get("production_path"):
        feedback_df, production_path = _normalize_feedback_df(config["production_path"])
        production_rows = len(feedback_df)
        if production_rows > 0:
            # Oversample feedback rows so user corrections have more influence
            # than the sparse bootstrap data. feedback_weight=10 means each
            # correction counts as 10 training examples.
            feedback_weight = int(config.get("feedback_weight", 5))
            feedback_df_weighted = pd.concat(
                [feedback_df] * feedback_weight, ignore_index=True
            )
            train_df = pd.concat([train_df, feedback_df_weighted], ignore_index=True)
            train_df = train_df.sort_values("date").reset_index(drop=True)
            log(
                f"loaded {production_rows} feedback rows "
                f"(oversampled {feedback_weight}x → {production_rows * feedback_weight} rows) "
                f"from {production_path}"
            )

    train_pd, test_pd, le, tfidf, feature_cols = _featurize_split(
        train_df, test_df, config
    )
    return {
        "train_pd": train_pd,
        "test_pd": test_pd,
        "label_encoder": le,
        "tfidf": tfidf,
        "feature_cols": feature_cols,
        "resolved_bootstrap_path": bootstrap_path,
        "resolved_production_path": production_path,
        "production_rows": production_rows,
    }


def save_bundle(bundle_dir, model_path, tfidf, label_encoder, feature_cols):
    bundle_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(model_path, bundle_dir / Path(model_path).name)
    joblib.dump(tfidf, bundle_dir / "tfidf_vectorizer.joblib")
    joblib.dump(label_encoder, bundle_dir / "label_encoder.joblib")
    metadata = {
        "text_column": TEXT_COL,
        "numeric_columns": NUMERIC_COLS,
        "feature_columns": feature_cols,
        "class_names": [str(c) for c in label_encoder.classes_],
        "normalization": "merchant uppercased, long numbers stripped, whitespace normalized",
        "model_family": "m1_ray_xgboost",
    }
    with open(bundle_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m1_ray.yaml")
    parser.add_argument("--mode", choices=["bootstrap", "retrain"], default="bootstrap")
    args = parser.parse_args()
    config = load_config(args.config)

    if "s3" in config:
        s3_cfg = config["s3"]
        access_key = os.environ.get("AWS_ACCESS_KEY_ID") or s3_cfg.get("access_key")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or s3_cfg.get("secret_key")
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or s3_cfg.get("endpoint_url")
        if access_key:
            os.environ["AWS_ACCESS_KEY_ID"] = access_key
        if secret_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
        if endpoint_url:
            os.environ["AWS_ENDPOINT_URL"] = endpoint_url

    tracking_uri = setup_mlflow(config)
    data_path = os.environ.get("M1_RAY_DATA_PATH", config["data_path"])
    production_path = (
        os.environ.get("M1_RAY_PRODUCTION_PATH")
        or (config.get("production_path") if args.mode == "retrain" else None)
    )
    quality_gate = float(
        os.environ.get("M1_RAY_GATE_FLOOR", config.get("quality_gate_macro_f1", 0.65))
    )
    registered_model_name = config.get("registered_model_name", "m1-ray-categorization")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"data_path={data_path}")
    log(f"production_path={production_path}")
    log(f"split_date={config['split_date']}")
    log(f"quality_gate_macro_f1={quality_gate}")

    ray.init(ignore_reinit_error=True, logging_level="ERROR")
    try:
        config = dict(config)
        config["data_path"] = data_path
        if production_path:
            config["production_path"] = production_path
        prepared = load_and_prepare(config, args.mode)
        train_pd = prepared["train_pd"]
        test_pd = prepared["test_pd"]
        le = prepared["label_encoder"]
        tfidf = prepared["tfidf"]
        feature_cols = prepared["feature_cols"]
        resolved_data_path = prepared["resolved_bootstrap_path"]
        resolved_production_path = prepared["resolved_production_path"]
        production_rows = prepared["production_rows"]
        num_classes = len(le.classes_)

        train_ds = ray.data.from_pandas(train_pd)
        valid_ds = ray.data.from_pandas(test_pd)

        params = {
            "objective": "multi:softprob",
            "num_class": num_classes,
            "max_depth": config["model"]["max_depth"],
            "learning_rate": config["model"]["learning_rate"],
            "eval_metric": "mlogloss",
            "tree_method": "hist",
        }

        trainer = XGBoostTrainer(
            label_column="label",
            params=params,
            num_boost_round=config["model"]["n_estimators"],
            scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
            datasets={"train": train_ds, "valid": valid_ds},
            run_config=RunConfig(
                name="m1_ray_xgb",
                storage_path=config["checkpoint_dir"],
                checkpoint_config=CheckpointConfig(
                    num_to_keep=2,
                    checkpoint_frequency=50,
                    checkpoint_at_end=True,
                ),
                failure_config=FailureConfig(max_failures=2),
            ),
        )

        with mlflow.start_run() as run:
            mlflow.log_params(
                {
                    "mode": args.mode,
                    "model_type": "xgboost_ray",
                    "data_path": data_path,
                    "resolved_data_path": resolved_data_path,
                    "production_path": production_path or "",
                    "resolved_production_path": resolved_production_path or "",
                    "production_rows": production_rows,
                    "feedback_weight": int(config.get("feedback_weight", 5)),
                    "production_rows_weighted": production_rows * int(config.get("feedback_weight", 5)),
                    "split_date": str(config["split_date"]),
                    "n_estimators": config["model"]["n_estimators"],
                    "max_depth": config["model"]["max_depth"],
                    "learning_rate": config["model"]["learning_rate"],
                    "tfidf_ngram_min": config["tfidf"]["ngram_min"],
                    "tfidf_ngram_max": config["tfidf"]["ngram_max"],
                    "tfidf_max_features": config["tfidf"]["max_features"],
                    "checkpoint_storage": config["checkpoint_dir"],
                    "fault_tolerance": "FailureConfig(max_failures=2)",
                    "quality_gate_macro_f1": quality_gate,
                    "ray_version": ray.__version__,
                    "python_version": platform.python_version(),
                    "platform": platform.platform(),
                }
            )

            start = time.time()
            result = trainer.fit()
            train_time = time.time() - start

            with result.checkpoint.as_directory() as ckpt_dir:
                model_path = os.path.join(ckpt_dir, "model.ubj")
                if not os.path.exists(model_path):
                    model_path = os.path.join(ckpt_dir, "model.json")

                bst = xgb.Booster()
                bst.load_model(model_path)

                x_test_df = test_pd.drop("label", axis=1)
                y_test = test_pd["label"].values
                dtest = xgb.DMatrix(x_test_df.values, feature_names=list(x_test_df.columns))
                pred_proba = bst.predict(dtest)
                if pred_proba.ndim == 1:
                    y_pred = pred_proba.astype(int)
                else:
                    y_pred = np.argmax(pred_proba, axis=1).astype(int)

                macro_f1 = f1_score(y_test, y_pred, average="macro", zero_division=0)
                weighted_f1 = f1_score(y_test, y_pred, average="weighted", zero_division=0)
                accuracy = accuracy_score(y_test, y_pred)

                mlflow.log_metrics(
                    {
                        "macro_f1": macro_f1,
                        "weighted_f1": weighted_f1,
                        "accuracy": accuracy,
                        "train_time_seconds": train_time,
                        "train_size": len(train_pd),
                        "test_size": len(test_pd),
                        "num_classes": num_classes,
                    }
                )

                present = np.unique(np.concatenate([y_test, y_pred]))
                report = classification_report(
                    y_test,
                    y_pred,
                    labels=present,
                    target_names=le.inverse_transform(present),
                    output_dict=True,
                    zero_division=0,
                )
                for cat, metrics in report.items():
                    if isinstance(metrics, dict):
                        safe = cat.replace(" ", "_").replace("/", "_")
                        mlflow.log_metric(f"f1_{safe}", metrics["f1-score"])

                # Safeguarding: fairness, explainability, robustness, privacy, accountability
                run_safeguarding_checks(
                    y_test=y_test,
                    y_pred=y_pred,
                    le=le,
                    tfidf=tfidf,
                    feature_cols=feature_cols,
                    pred_proba=pred_proba if pred_proba.ndim > 1 else None,
                    data_path=resolved_data_path,
                )

                mlflow.xgboost.log_model(bst, "model")

                with tempfile.TemporaryDirectory() as tmpdir:
                    bundle_dir = Path(tmpdir) / "bundle"
                    save_bundle(bundle_dir, model_path, tfidf, le, feature_cols)
                    mlflow.log_artifacts(str(bundle_dir), artifact_path="bundle")

                absolute_passed = macro_f1 >= quality_gate
                prev_version, prev_metric = get_latest_production_metric(
                    registered_model_name, "macro_f1"
                )
                log(f"previous version: v{prev_version} macro_f1={prev_metric}")

                do_register, reason = should_register(
                    mode=args.mode,
                    current_metric=macro_f1,
                    previous_metric=prev_metric,
                    higher_is_better=True,
                    absolute_gate_passed=absolute_passed,
                    prefix="M1-RAY",
                )

                mlflow.set_tag("quality_gate_metric", "macro_f1")
                mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
                mlflow.set_tag("registered", str(do_register).lower())
                mlflow.set_tag("register_reason", reason)
                mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
                mlflow.set_tag("mode", args.mode)

                if do_register:
                    client = MlflowClient()
                    mv = register_model_version(
                        client, registered_model_name, run.info.run_id, "bundle"
                    )
                    log(f"REGISTERED v{mv.version} — {reason}")
                else:
                    log(f"NOT REGISTERED — {reason}")

                log(
                    f"macro_f1={macro_f1:.4f}, accuracy={accuracy:.4f}, "
                    f"weighted_f1={weighted_f1:.4f}, train_time={train_time:.1f}s"
                )
                log(f"done | run_id={run.info.run_id}")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
