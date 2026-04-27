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
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score, classification_report, f1_score, log_loss, top_k_accuracy_score
from sklearn.preprocessing import LabelEncoder
from scipy.sparse import csr_matrix, hstack

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _common import (  # noqa: E402
    check_category_regression,
    get_per_category_f1_from_run,
    load_config,
    log as _log,
    register_model_version,
    resolve_path,
    setup_mlflow,
)
from safeguarding import run_safeguarding_checks  # noqa: E402

os.environ["GIT_PYTHON_REFRESH"] = "quiet"


def log(msg):
    _log("M1-RAY", msg)


TEXT_COL = "merchant"
LABEL_COL = "category"
NUMERIC_COLS = ["log_amount", "day_of_week", "day_of_month"]
CANONICAL_COLUMNS = ["date", TEXT_COL, "amount", LABEL_COL]
METRIC_EPS = 1e-6


def _get_latest_registered_run_metrics(model_name: str) -> tuple[int | None, dict]:
    """Return (latest_version, latest_run_metrics) for a registered model."""
    try:
        client = MlflowClient()
        versions = client.search_model_versions(f"name='{model_name}'")
        if not versions:
            return None, {}
        latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        run = client.get_run(latest.run_id)
        return int(latest.version), dict(run.data.metrics)
    except Exception as e:
        log(f"[WARN] could not fetch latest registered run metrics for {model_name}: {e}")
        return None, {}


def _decide_m1_registration(
    *,
    mode: str,
    absolute_gate_passed: bool,
    category_gate_passed: bool,
    top3_accuracy: float,
    weighted_f1: float,
    multiclass_log_loss: float,
    real_world_macro_f1: float | None,
    previous_version: int | None,
    previous_metrics: dict,
) -> tuple[bool, str, str]:
    """Registration decision with saturated-metric tie breaks.

    Primary comparator:
      1. real_world_macro_f1 when available for both current and previous model
      2. otherwise top3_accuracy

    Tie breaks when primary metric ties within epsilon:
      1. lower log_loss
      2. higher weighted_f1
    """
    if not absolute_gate_passed:
        return False, f"absolute gate failed (top3_accuracy={top3_accuracy:.4f})", "top3_accuracy"

    if not category_gate_passed:
        return False, "per-category regression gate failed (>2% relative drop in ≥1 category)", "top3_accuracy"

    if previous_version is None:
        metric_name = "real_world_macro_f1" if real_world_macro_f1 is not None else "top3_accuracy"
        current_value = real_world_macro_f1 if real_world_macro_f1 is not None else top3_accuracy
        return True, f"no previous version, registering (current={current_value:.4f})", metric_name

    prev_rw = previous_metrics.get("real_world_macro_f1")
    prev_top3 = previous_metrics.get("top3_accuracy")
    prev_log_loss = previous_metrics.get("log_loss")
    prev_weighted_f1 = previous_metrics.get("weighted_f1")

    if real_world_macro_f1 is not None and prev_rw is not None:
        metric_name = "real_world_macro_f1"
        current_primary = real_world_macro_f1
        previous_primary = prev_rw
    else:
        metric_name = "top3_accuracy"
        current_primary = top3_accuracy
        previous_primary = prev_top3

    if previous_primary is None:
        return True, (
            f"previous version missing {metric_name}, registering "
            f"(current={current_primary:.4f})"
        ), metric_name

    if current_primary > previous_primary + METRIC_EPS:
        return True, (
            f"improved {metric_name} "
            f"(prev={previous_primary:.4f} -> curr={current_primary:.4f})"
        ), metric_name

    if current_primary + METRIC_EPS < previous_primary:
        return False, (
            f"no improvement in {metric_name} "
            f"(prev={previous_primary:.4f} -> curr={current_primary:.4f})"
        ), metric_name

    if prev_log_loss is not None and multiclass_log_loss < prev_log_loss - METRIC_EPS:
        return True, (
            f"tied {metric_name} at {current_primary:.4f}; "
            f"log_loss improved (prev={prev_log_loss:.4f} -> curr={multiclass_log_loss:.4f})"
        ), metric_name

    if prev_weighted_f1 is not None and weighted_f1 > prev_weighted_f1 + METRIC_EPS:
        return True, (
            f"tied {metric_name} at {current_primary:.4f}; "
            f"weighted_f1 improved (prev={prev_weighted_f1:.4f} -> curr={weighted_f1:.4f})"
        ), metric_name

    return False, (
        f"tied {metric_name} at {current_primary:.4f} with no secondary improvement "
        f"(log_loss prev={prev_log_loss if prev_log_loss is not None else 'n/a'} "
        f"curr={multiclass_log_loss:.4f}; weighted_f1 prev="
        f"{prev_weighted_f1 if prev_weighted_f1 is not None else 'n/a'} curr={weighted_f1:.4f})"
    ), metric_name


@ray.remote(max_retries=2)
def _ray_train_xgb(x_train, y_train, x_test, y_test, feature_cols, params, num_boost_round):
    # DMatrix is not picklable — rebuild it inside the worker so sparse CSR
    # semantics (missing != 0) match the serving path.
    dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=feature_cols)
    deval = xgb.DMatrix(x_test, label=y_test, feature_names=feature_cols)
    evals_result: dict = {}
    bst = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtrain, "train"), (deval, "eval")],
        evals_result=evals_result,
        verbose_eval=50,
    )
    return bst.save_raw(), evals_result


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


def _normalize_processed_categorization_df(path, source_name="bootstrap"):
    local_path = resolve_path(path, "M1-RAY")
    df = pd.read_csv(local_path)
    df = df.rename(
        columns={
            "project_category": LABEL_COL,
            "payee_name": TEXT_COL,
            "imported_payee": TEXT_COL,
        }
    )
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df[LABEL_COL] = df[LABEL_COL].astype(str).str.strip()
    df[TEXT_COL] = df[TEXT_COL].astype(str).str.strip()
    df = df.dropna(subset=["date", "amount", LABEL_COL, TEXT_COL])
    df = df[df[LABEL_COL] != ""]
    df = df[df[TEXT_COL] != ""]
    df = df[CANONICAL_COLUMNS].copy()
    df["source"] = source_name
    return df.sort_values("date").reset_index(drop=True), local_path


def _normalize_any_supervised_df(path, source_name="bootstrap"):
    local_path = resolve_path(path, "M1-RAY")
    head = pd.read_csv(local_path, nrows=5)
    cols = {c.strip().lower() for c in head.columns}
    if {"debit amount", "credit amount", "category"} & set(head.columns):
        return _normalize_bootstrap_df(path)
    if {"date", "merchant", "amount"} <= cols and (
        "project_category" in cols or "category" in cols
    ):
        return _normalize_processed_categorization_df(path, source_name=source_name)
    raise ValueError(
        f"Unsupported M1 supervised dataset schema for {local_path}. "
        "Expected either moneydata-style debit/credit export or processed categorization CSV."
    )


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


def _split_feedback_for_eval(feedback_df, config):
    fraction = float(
        os.environ.get(
            "M1_RAY_FEEDBACK_EVAL_FRACTION",
            config.get("feedback_eval_fraction", 0.2),
        )
    )
    fraction = max(0.0, min(fraction, 0.5))
    if len(feedback_df) < 2 or fraction <= 0:
        return feedback_df.copy(), feedback_df.iloc[0:0].copy(), fraction

    eval_rows = max(1, int(len(feedback_df) * fraction))
    eval_rows = min(eval_rows, len(feedback_df) - 1)
    feedback_df = feedback_df.sort_values("date").reset_index(drop=True)
    train_feedback = feedback_df.iloc[:-eval_rows].copy()
    eval_feedback = feedback_df.iloc[-eval_rows:].copy()
    return train_feedback, eval_feedback, fraction


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

    x_train_num = csr_matrix(train_df[NUMERIC_COLS].fillna(0).values, dtype=np.float32)
    x_test_num = csr_matrix(test_df[NUMERIC_COLS].fillna(0).values, dtype=np.float32)

    # Keep sparse — avoids dense allocation of N×F float32 (~2 GB for 500k rows)
    x_train = hstack([x_train_text, x_train_num], format="csr").astype(np.float32)
    x_test = hstack([x_test_text, x_test_num], format="csr").astype(np.float32)

    feature_cols = [f"f{i}" for i in range(x_train.shape[1])]

    # Mask of which surviving test rows came from real user feedback (vs synthetic
    # eval). Used in main() to compute a separate real-world macro_f1 gate.
    if "source" in test_df.columns:
        test_is_feedback = (test_df["source"].fillna("") == "production_feedback").values
    else:
        test_is_feedback = np.zeros(len(test_df), dtype=bool)

    return (
        x_train,
        train_df["label"].values,
        x_test,
        test_df["label"].values,
        le,
        tfidf,
        feature_cols,
        test_is_feedback,
    )


def _apply_row_caps(train_df, test_df, config):
    max_train_rows = os.environ.get("M1_RAY_MAX_TRAIN_ROWS", config.get("max_train_rows"))
    max_eval_rows = os.environ.get("M1_RAY_MAX_EVAL_ROWS", config.get("max_eval_rows"))

    train_df = train_df.copy()
    test_df = test_df.copy()

    if max_train_rows:
        max_train_rows = int(max_train_rows)
        if len(train_df) > max_train_rows:
            train_df = (
                train_df.sample(n=max_train_rows, random_state=42)
                .sort_values("date")
                .reset_index(drop=True)
            )
            log(f"sampled training rows to {len(train_df):,}")

    if max_eval_rows:
        max_eval_rows = int(max_eval_rows)
        if len(test_df) > max_eval_rows:
            test_df = (
                test_df.sample(n=max_eval_rows, random_state=42)
                .sort_values("date")
                .reset_index(drop=True)
            )
            log(f"sampled eval rows to {len(test_df):,}")

    return train_df, test_df


def load_and_prepare(config, mode):
    bootstrap_df, bootstrap_path = _normalize_any_supervised_df(
        config["data_path"], source_name="bootstrap"
    )

    eval_path = config.get("eval_path") or os.environ.get("M1_RAY_EVAL_PATH")
    if eval_path:
        test_df, eval_resolved_path = _normalize_any_supervised_df(
            eval_path, source_name="explicit_eval"
        )
        train_df = bootstrap_df.copy()
    else:
        # No explicit eval_path: fall back to a chronological 80/20 split
        split_idx = int(len(bootstrap_df) * 0.8)
        train_df = bootstrap_df.iloc[:split_idx].copy()
        test_df = bootstrap_df.iloc[split_idx:].copy()
        eval_resolved_path = None

    production_path = None
    production_rows = 0
    production_train_rows = 0
    production_eval_rows = 0
    feedback_eval_fraction = 0.0
    if mode == "retrain" and config.get("production_path"):
        feedback_df, production_path = _normalize_feedback_df(config["production_path"])
        production_rows = len(feedback_df)
        if production_rows > 0:
            train_feedback_df, eval_feedback_df, feedback_eval_fraction = _split_feedback_for_eval(
                feedback_df, config
            )
            production_train_rows = len(train_feedback_df)
            production_eval_rows = len(eval_feedback_df)
            # Oversample feedback rows so user corrections have more influence
            # than the sparse bootstrap data. feedback_weight=10 means each
            # correction counts as 10 training examples.
            feedback_weight = int(config.get("feedback_weight", 5))
            if production_train_rows > 0:
                feedback_df_weighted = pd.concat(
                    [train_feedback_df] * feedback_weight, ignore_index=True
                )
                train_df = pd.concat([train_df, feedback_df_weighted], ignore_index=True)
                train_df = train_df.sort_values("date").reset_index(drop=True)
            if production_eval_rows > 0:
                test_df = pd.concat([test_df, eval_feedback_df], ignore_index=True)
                test_df = test_df.sort_values("date").reset_index(drop=True)
            log(
                f"loaded {production_rows} feedback rows "
                f"(train={production_train_rows}, eval={production_eval_rows}, "
                f"oversampled train {feedback_weight}x → {production_train_rows * feedback_weight} rows) "
                f"from {production_path}"
            )

    train_df, test_df = _apply_row_caps(train_df, test_df, config)
    x_train, y_train, x_test, y_test, le, tfidf, feature_cols, test_is_feedback = (
        _featurize_split(train_df, test_df, config)
    )
    return {
        "x_train": x_train,
        "y_train": y_train,
        "x_test": x_test,
        "y_test": y_test,
        "train_size": x_train.shape[0],
        "test_size": x_test.shape[0],
        "label_encoder": le,
        "tfidf": tfidf,
        "feature_cols": feature_cols,
        "test_is_feedback": test_is_feedback,
        "resolved_bootstrap_path": bootstrap_path,
        "resolved_eval_path": eval_resolved_path,
        "resolved_production_path": production_path,
        "production_rows": production_rows,
        "production_train_rows": production_train_rows,
        "production_eval_rows": production_eval_rows,
        "feedback_eval_fraction": feedback_eval_fraction,
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

    ray.init(ignore_reinit_error=True, logging_level="ERROR")
    log(f"ray initialized: version={ray.__version__}")

    tracking_uri = setup_mlflow(config)
    data_path = os.environ.get("M1_RAY_DATA_PATH", config["data_path"])
    eval_path = os.environ.get("M1_RAY_EVAL_PATH", config.get("eval_path", ""))
    production_path = (
        os.environ.get("M1_RAY_PRODUCTION_PATH")
        or (config.get("production_path") if args.mode == "retrain" else None)
    )
    quality_gate = float(
        os.environ.get(
            "M1_RAY_GATE_TOP3",
            os.environ.get(
                "M1_RAY_GATE_FLOOR",
                config.get("quality_gate_top3_accuracy", config.get("quality_gate_macro_f1", 0.65)),
            ),
        )
    )
    real_world_floor = float(
        os.environ.get(
            "M1_RAY_REAL_WORLD_GATE_FLOOR",
            config.get("real_world_gate_floor", 0.40),
        )
    )
    real_world_min_rows = int(
        os.environ.get(
            "M1_RAY_REAL_WORLD_MIN_ROWS",
            config.get("real_world_min_rows", 20),
        )
    )
    registered_model_name = config.get("registered_model_name", "m1-ray-categorization")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"data_path={data_path}")
    log(f"eval_path={eval_path or None}")
    log(f"production_path={production_path}")
    log(f"eval_path={eval_path or '(80/20 chronological split)'}")
    log(f"quality_gate_top3_accuracy={quality_gate}")

    config = dict(config)
    config["data_path"] = data_path
    if eval_path:
        config["eval_path"] = eval_path
    if production_path:
        config["production_path"] = production_path

    prepared = load_and_prepare(config, args.mode)
    x_train   = prepared["x_train"]
    y_train   = prepared["y_train"]
    x_test    = prepared["x_test"]
    y_test    = prepared["y_test"]
    le        = prepared["label_encoder"]
    tfidf     = prepared["tfidf"]
    feature_cols          = prepared["feature_cols"]
    resolved_data_path    = prepared["resolved_bootstrap_path"]
    resolved_eval_path    = prepared["resolved_eval_path"]
    resolved_production_path = prepared["resolved_production_path"]
    production_rows       = prepared["production_rows"]
    production_train_rows = prepared["production_train_rows"]
    production_eval_rows  = prepared["production_eval_rows"]
    feedback_eval_fraction = prepared["feedback_eval_fraction"]
    num_classes = len(le.classes_)

    log(f"training: train={x_train.shape[0]:,} rows  eval={x_test.shape[0]:,} rows  features={x_train.shape[1]}")

    params = {
        "objective":        "multi:softprob",
        "num_class":        num_classes,
        "max_depth":        config["model"]["max_depth"],
        "learning_rate":    config["model"]["learning_rate"],
        "subsample":        float(config["model"].get("subsample", 1.0)),
        "colsample_bytree": float(config["model"].get("colsample_bytree", 1.0)),
        "min_child_weight": int(config["model"].get("min_child_weight", 1)),
        "gamma":            float(config["model"].get("gamma", 0.0)),
        "eval_metric":      "mlogloss",
        "tree_method":      "hist",
        "nthread":          -1,
    }

    with mlflow.start_run() as run:
        mlflow.log_params({
            "mode":                    args.mode,
            "model_type":              "xgboost_sparse",
            "data_path":               data_path,
            "resolved_data_path":      resolved_data_path,
            "eval_path":               eval_path or "",
            "resolved_eval_path":      resolved_eval_path or "",
            "production_path":         production_path or "",
            "resolved_production_path": resolved_production_path or "",
            "production_rows":         production_rows,
            "production_train_rows":   production_train_rows,
            "production_eval_rows":    production_eval_rows,
            "feedback_weight":         int(config.get("feedback_weight", 5)),
            "feedback_eval_fraction":  feedback_eval_fraction,
            "production_rows_weighted": production_train_rows * int(config.get("feedback_weight", 5)),
            "eval_split":              eval_path or "80/20_chronological",
            "n_estimators":            config["model"]["n_estimators"],
            "max_depth":               config["model"]["max_depth"],
            "learning_rate":           config["model"]["learning_rate"],
            "subsample":               params["subsample"],
            "colsample_bytree":        params["colsample_bytree"],
            "min_child_weight":        params["min_child_weight"],
            "gamma":                   params["gamma"],
            "tfidf_ngram_min":         config["tfidf"]["ngram_min"],
            "tfidf_ngram_max":         config["tfidf"]["ngram_max"],
            "tfidf_max_features":      config["tfidf"]["max_features"],
            "quality_gate_top3_accuracy": quality_gate,
            "real_world_gate_floor":   real_world_floor,
            "real_world_min_rows":     real_world_min_rows,
            "python_version":          platform.python_version(),
            "platform":                platform.platform(),
            "ray_version":             ray.__version__,
        })

        start = time.time()
        raw_bytes, evals_result = ray.get(
            _ray_train_xgb.remote(
                x_train, y_train, x_test, y_test, feature_cols, params,
                config["model"]["n_estimators"],
            )
        )
        bst = xgb.Booster()
        bst.load_model(bytearray(raw_bytes))
        train_time = time.time() - start

        deval = xgb.DMatrix(x_test, label=y_test, feature_names=feature_cols)
        del x_train, x_test

        with tempfile.TemporaryDirectory() as tmpdir:
            model_path = os.path.join(tmpdir, "model.ubj")
            bst.save_model(model_path)

            pred_proba = bst.predict(deval)
            if pred_proba.ndim == 1:
                y_pred = pred_proba.astype(int)
            else:
                y_pred = np.argmax(pred_proba, axis=1).astype(int)

            macro_f1    = f1_score(y_test, y_pred, average="macro",    zero_division=0)
            weighted_f1 = f1_score(y_test, y_pred, average="weighted", zero_division=0)
            accuracy    = accuracy_score(y_test, y_pred)
            top3_accuracy = top_k_accuracy_score(
                y_test, pred_proba, k=min(3, num_classes), labels=np.arange(num_classes)
            )
            multiclass_log_loss = log_loss(y_test, pred_proba, labels=np.arange(num_classes))

            mlflow.log_metrics({
                "macro_f1":          macro_f1,
                "weighted_f1":       weighted_f1,
                "accuracy":          accuracy,
                "top3_accuracy":     top3_accuracy,
                "log_loss":          multiclass_log_loss,
                "train_time_seconds": train_time,
                "train_size":        prepared["train_size"],
                "test_size":         prepared["test_size"],
                "num_classes":       num_classes,
            })

            # Real-world macro_f1 — only over user-feedback rows in the eval split.
            # The synthetic eval has near-perfect merchant overlap with training so
            # the headline macro_f1 is essentially memorization. The real-world
            # subset (held-out user corrections) is what tells us if the model
            # generalizes to merchants it actually sees in production.
            test_is_feedback = prepared["test_is_feedback"]
            real_world_eval_rows = int(test_is_feedback.sum())
            if real_world_eval_rows >= real_world_min_rows:
                rw_y_true = y_test[test_is_feedback]
                rw_y_pred = y_pred[test_is_feedback]
                real_world_macro_f1 = float(
                    f1_score(rw_y_true, rw_y_pred, average="macro", zero_division=0)
                )
                real_world_gate_passed = real_world_macro_f1 >= real_world_floor
                real_world_skip_reason = ""
                mlflow.log_metric("real_world_macro_f1", real_world_macro_f1)
                log(
                    f"real_world_macro_f1={real_world_macro_f1:.4f} "
                    f"(over {real_world_eval_rows} feedback rows; floor={real_world_floor:.2f})"
                )
            else:
                real_world_macro_f1 = None
                # Skip the gate when there isn't enough feedback to fail on it.
                # First few retrains will hit this branch; that's fine — we don't
                # want a 5-row noisy F1 to block registration.
                real_world_gate_passed = True
                real_world_skip_reason = (
                    f"only {real_world_eval_rows} feedback eval rows < "
                    f"{real_world_min_rows} required"
                )
                log(f"real_world gate skipped — {real_world_skip_reason}")
            mlflow.log_metric("real_world_eval_rows", real_world_eval_rows)
            mlflow.set_tag("real_world_gate_passed", str(real_world_gate_passed).lower())
            mlflow.set_tag("real_world_gate_floor", str(real_world_floor))
            mlflow.set_tag("real_world_min_rows", str(real_world_min_rows))
            if real_world_skip_reason:
                mlflow.set_tag("real_world_gate_skipped_reason", real_world_skip_reason)

            present = np.unique(np.concatenate([y_test, y_pred]))
            report = classification_report(
                y_test, y_pred,
                labels=present,
                target_names=le.inverse_transform(present),
                output_dict=True,
                zero_division=0,
            )
            for cat, metrics in report.items():
                if isinstance(metrics, dict):
                    safe = cat.replace(" ", "_").replace("/", "_")
                    mlflow.log_metric(f"f1_{safe}", metrics["f1-score"])

            run_safeguarding_checks(
                y_test=y_test, y_pred=y_pred, le=le, tfidf=tfidf,
                feature_cols=feature_cols,
                pred_proba=pred_proba if pred_proba.ndim > 1 else None,
                data_path=resolved_data_path,
            )

            mlflow.xgboost.log_model(bst, "model")

            bundle_dir = Path(tmpdir) / "bundle"
            save_bundle(bundle_dir, model_path, tfidf, le, feature_cols)
            mlflow.log_artifacts(str(bundle_dir), artifact_path="bundle")

            top3_passed = top3_accuracy >= quality_gate
            # Combined absolute gate: legacy top3 floor AND new real-world floor.
            # The real-world piece auto-passes when there's not enough feedback
            # data (real_world_min_rows), so this is backward-compatible.
            absolute_passed = top3_passed and real_world_gate_passed
            client_for_gate = MlflowClient()
            prev_version, prev_metrics = _get_latest_registered_run_metrics(registered_model_name)
            prev_top3 = prev_metrics.get("top3_accuracy")
            prev_rw = prev_metrics.get("real_world_macro_f1")
            prev_log_loss = prev_metrics.get("log_loss")
            prev_weighted_f1 = prev_metrics.get("weighted_f1")
            log(
                f"previous version: v{prev_version} "
                f"top3_accuracy={prev_top3} "
                f"real_world_macro_f1={prev_rw} "
                f"log_loss={prev_log_loss} "
                f"weighted_f1={prev_weighted_f1}"
            )

            prev_per_cat_f1 = get_per_category_f1_from_run(client_for_gate, registered_model_name)
            category_gate_passed, regressed_cats = check_category_regression(
                report=report,
                prev_per_cat_f1=prev_per_cat_f1,
                regression_threshold=0.02,
            )
            if not category_gate_passed:
                log(f"CATEGORY REGRESSION GATE FAILED: {regressed_cats}")
            else:
                log(f"category regression gate passed (checked {len(prev_per_cat_f1)} categories)")

            do_register, reason, comparison_metric = _decide_m1_registration(
                mode=args.mode,
                absolute_gate_passed=absolute_passed,
                category_gate_passed=category_gate_passed,
                top3_accuracy=top3_accuracy,
                weighted_f1=weighted_f1,
                multiclass_log_loss=multiclass_log_loss,
                real_world_macro_f1=real_world_macro_f1,
                previous_version=prev_version,
                previous_metrics=prev_metrics,
            )

            mlflow.set_tag("quality_gate_metric", comparison_metric)
            mlflow.set_tag("top3_gate_passed", str(top3_passed).lower())
            mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
            mlflow.set_tag("category_regression_gate_passed", str(category_gate_passed).lower())
            mlflow.set_tag("regressed_categories", ",".join(regressed_cats) if regressed_cats else "none")
            mlflow.set_tag("registered", str(do_register).lower())
            mlflow.set_tag("register_reason", reason)
            mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
            mlflow.set_tag("comparison_metric", comparison_metric)
            mlflow.set_tag("mode", args.mode)

            if do_register:
                client = MlflowClient()
                mv = register_model_version(client, registered_model_name, run.info.run_id, "bundle")
                log(f"REGISTERED v{mv.version} — {reason}")
            else:
                log(f"NOT REGISTERED — {reason}")

            rw_str = (
                f"real_world_macro_f1={real_world_macro_f1:.4f} (n={real_world_eval_rows})"
                if real_world_macro_f1 is not None
                else f"real_world_macro_f1=skipped (n={real_world_eval_rows})"
            )
            log(
                f"top3_accuracy={top3_accuracy:.4f}, macro_f1={macro_f1:.4f}, "
                f"accuracy={accuracy:.4f}, weighted_f1={weighted_f1:.4f}, "
                f"log_loss={multiclass_log_loss:.4f}, {rw_str}, "
                f"train_time={train_time:.1f}s"
            )
            log(f"done | run_id={run.info.run_id}")

    ray.shutdown()


if __name__ == "__main__":
    main()
