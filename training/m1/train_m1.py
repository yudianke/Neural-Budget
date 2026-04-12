import argparse
import os
import platform
import sys
import time

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.tracking import MlflowClient
from scipy.sparse import csr_matrix, hstack
from sklearn.dummy import DummyClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.preprocessing import LabelEncoder

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _common import (  # noqa: E402
    get_latest_production_metric,
    load_and_combine,
    load_config,
    log as _log,
    register_model_version,
    resolve_path,
    setup_mlflow,
    should_register,
)


def log(msg):
    _log("M1", msg)


NUMERIC_COLS = [
    "log_abs_amount",
    "day_of_week",
    "day_of_month",
    "month",
    "repeat_count",
    "is_recurring_candidate",
]
TEXT_COL = "merchant"
LABEL_COL = "project_category"


def load_synthetic(bootstrap_path, production_path):
    df = load_and_combine(bootstrap_path, production_path, prefix="M1")
    df = df.dropna(subset=[LABEL_COL, TEXT_COL])
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def load_real_eval(path):
    local = resolve_path(path, "M1")
    df = pd.read_csv(local)
    df = df.dropna(subset=[LABEL_COL, TEXT_COL, "amount", "date"])
    df["date"] = pd.to_datetime(df["date"])
    df["log_abs_amount"] = np.log1p(df["amount"].abs())
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_month"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["repeat_count"] = 0
    df["is_recurring_candidate"] = 0
    return df


def build_model(config):
    tfidf = TfidfVectorizer(
        analyzer=config["tfidf"].get("analyzer", "word"),
        ngram_range=(config["tfidf"]["ngram_min"], config["tfidf"]["ngram_max"]),
        max_features=config["tfidf"]["max_features"],
        lowercase=True,
    )
    mt = config["model"]["type"]
    if mt == "baseline_dummy":
        clf = DummyClassifier(strategy="most_frequent")
    elif mt == "baseline_logreg":
        clf = LogisticRegression(max_iter=2000, C=1.0)
    elif mt == "xgboost":
        clf = xgb.XGBClassifier(
            n_estimators=config["model"]["n_estimators"],
            max_depth=config["model"]["max_depth"],
            learning_rate=config["model"]["learning_rate"],
            eval_metric="mlogloss",
            tree_method="hist",
            n_jobs=-1,
        )
    else:
        raise ValueError(f"unknown model type {mt}")
    return tfidf, clf


def featurize(tfidf, df, fit=False):
    text_vec = (
        tfidf.fit_transform(df[TEXT_COL].astype(str))
        if fit
        else tfidf.transform(df[TEXT_COL].astype(str))
    )
    num_cols = [c for c in NUMERIC_COLS if c in df.columns]
    if num_cols:
        num_vec = csr_matrix(df[num_cols].values.astype(float))
        return hstack([text_vec, num_vec])
    return text_vec


def evaluate(clf, tfidf, df, le, label):
    known_mask = df[LABEL_COL].isin(le.classes_)
    n_filtered = int((~known_mask).sum())
    df_eval = df[known_mask].reset_index(drop=True)
    if len(df_eval) == 0:
        return 0.0, 0.0, 0
    X = featurize(tfidf, df_eval, fit=False)
    y_true = le.transform(df_eval[LABEL_COL])
    y_pred = clf.predict(X)
    macro = f1_score(y_true, y_pred, average="macro", zero_division=0)
    weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    log(f"{label}: macro_f1={macro:.4f} weighted_f1={weighted:.4f} n={len(df_eval)} filtered={n_filtered}")
    return macro, weighted, len(df_eval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m1.yaml")
    parser.add_argument("--mode", choices=["bootstrap", "retrain"], default="bootstrap")
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = setup_mlflow(config)
    train_path = os.environ.get("M1_TRAIN_PATH", config["train_path"])
    synth_eval_path = os.environ.get("M1_EVAL_PATH", config["eval_path"])
    real_eval_path = os.environ.get("M1_REAL_EVAL_PATH", config["real_eval_path"])
    production_path = config.get("production_path") if args.mode == "retrain" else None
    gate_floor = float(os.environ.get("M1_GATE_FLOOR", config.get("quality_gate_floor", 0.55)))
    gate_ceiling = float(os.environ.get("M1_GATE_CEILING", config.get("quality_gate_ceiling", 0.98)))
    registered_model_name = config.get("registered_model_name", "m1-categorization")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"production_path={production_path}")
    log(f"gate floor={gate_floor} ceiling={gate_ceiling}")

    train_df = load_synthetic(train_path, production_path)
    synth_eval_df = load_synthetic(synth_eval_path, None)
    real_eval_df = load_real_eval(real_eval_path)

    le = LabelEncoder()
    y_train = le.fit_transform(train_df[LABEL_COL])

    tfidf, clf = build_model(config)
    X_train = featurize(tfidf, train_df, fit=True)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "mode": args.mode,
                "model_type": config["model"]["type"],
                "tfidf_ngram_min": config["tfidf"]["ngram_min"],
                "tfidf_ngram_max": config["tfidf"]["ngram_max"],
                "tfidf_max_features": config["tfidf"]["max_features"],
                "train_size": len(train_df),
                "synth_eval_size": len(synth_eval_df),
                "real_eval_size": len(real_eval_df),
                "num_classes_trained": len(le.classes_),
                "train_path": train_path,
                "production_path": production_path or "",
                "gate_floor": gate_floor,
                "gate_ceiling": gate_ceiling,
                "python_version": platform.python_version(),
                "platform": platform.platform(),
            }
        )
        if config["model"]["type"] == "xgboost":
            mlflow.log_params(
                {
                    "n_estimators": config["model"]["n_estimators"],
                    "max_depth": config["model"]["max_depth"],
                    "learning_rate": config["model"]["learning_rate"],
                }
            )

        start = time.time()
        clf.fit(X_train, y_train)
        train_time = time.time() - start
        mlflow.log_metric("train_time_seconds", train_time)

        synth_macro, synth_weighted, _ = evaluate(clf, tfidf, synth_eval_df, le, "synthetic_eval")
        real_macro, real_weighted, _ = evaluate(clf, tfidf, real_eval_df, le, "real_eval")

        mlflow.log_metric("synth_macro_f1", synth_macro)
        mlflow.log_metric("synth_weighted_f1", synth_weighted)
        mlflow.log_metric("real_macro_f1", real_macro)
        mlflow.log_metric("real_weighted_f1", real_weighted)

        absolute_passed = gate_floor <= real_macro <= gate_ceiling
        prev_version, prev_metric = get_latest_production_metric(registered_model_name, "real_macro_f1")
        log(f"previous version: v{prev_version} real_macro_f1={prev_metric}")

        do_register, reason = should_register(
            mode=args.mode,
            current_metric=real_macro,
            previous_metric=prev_metric,
            higher_is_better=True,
            absolute_gate_passed=absolute_passed,
            prefix="M1",
        )

        mlflow.set_tag("quality_gate_metric", "real_macro_f1")
        mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
        mlflow.set_tag("registered", str(do_register).lower())
        mlflow.set_tag("register_reason", reason)
        mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
        mlflow.set_tag("mode", args.mode)

        if do_register:
            mlflow.sklearn.log_model(clf, artifact_path="model")
            client = MlflowClient()
            mv = register_model_version(client, registered_model_name, run.info.run_id, "model")
            log(f"REGISTERED v{mv.version} — {reason}")
        else:
            mlflow.sklearn.log_model(clf, artifact_path="model")
            log(f"NOT REGISTERED — {reason}")

        log(f"done | run_id={run.info.run_id}")


if __name__ == "__main__":
    main()
