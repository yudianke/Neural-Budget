import argparse
import os
import platform
import sys
import tempfile
import time
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _common import (  # noqa
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

# -----------------------------
# Feature columns
# -----------------------------
TEXT_COL = "merchant"
LABEL_COL = "project_category"

NUMERIC_COLS = [
    "log_abs_amount",
    "day_of_week",
    "day_of_month",
    "month",
    "repeat_count",
    "is_recurring_candidate",
]

CATEGORICAL_COLS = [
    "transaction_type",
    "persona_cluster",
]

# -----------------------------
# Preprocessing
# -----------------------------
def normalize_merchant(text: str) -> str:
    if pd.isna(text):
        return "UNKNOWN"
    return (
        str(text)
        .upper()
        .replace("&", " AND ")
    )

def preprocess_df(df):
    # Normalize merchant
    df[TEXT_COL] = (
        df[TEXT_COL]
        .astype(str)
        .str.upper()
        .str.replace(r"[^A-Z0-9 ]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # Numeric features
    for col in NUMERIC_COLS:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Categorical features
    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].astype(str).fillna("unknown")

    return df

# -----------------------------
# Data loaders
# -----------------------------
def load_synthetic(train_path, production_path):
    df = load_and_combine(train_path, production_path, prefix="M1")
    df = df.dropna(subset=[LABEL_COL, TEXT_COL])
    return preprocess_df(df)

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

    return preprocess_df(df)

# -----------------------------
# Model builder
# -----------------------------
def build_model(config):

    tfidf = TfidfVectorizer(
        analyzer="char_wb",            # key improvement
        ngram_range=(3, 5),
        max_features=config["tfidf"]["max_features"],
        lowercase=True,
        min_df=2,
    )

    model_type = config["model"]["type"]

    if model_type == "baseline_dummy":
        clf = DummyClassifier(strategy="most_frequent")

    elif model_type == "baseline_logreg":
        clf = LogisticRegression(max_iter=2000)

    elif model_type == "xgboost":
        clf = xgb.XGBClassifier(
            n_estimators=config["model"]["n_estimators"],
            max_depth=config["model"]["max_depth"],
            learning_rate=config["model"]["learning_rate"],
            subsample=0.9,
            colsample_bytree=0.9,
            objective="multi:softprob",
            eval_metric="mlogloss",
            tree_method="hist",
            n_jobs=-1,
        )

    else:
        raise ValueError(f"Unknown model type {model_type}")

    preprocessor = ColumnTransformer(
        [
            ("text", tfidf, TEXT_COL),
            ("num", "passthrough", NUMERIC_COLS),
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_COLS),
        ],
        sparse_threshold=1.0,
    )

    return Pipeline([
        ("features", preprocessor),
        ("clf", clf),
    ])

# -----------------------------
# Evaluation
# -----------------------------
def evaluate(pipeline, df, le, label):
    known_mask = df[LABEL_COL].isin(le.classes_)
    df_eval = df[known_mask]

    if len(df_eval) == 0:
        return 0, 0, 0

    y_true = le.transform(df_eval[LABEL_COL])
    y_pred = pipeline.predict(df_eval)

    macro = f1_score(y_true, y_pred, average="macro", zero_division=0)
    weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)

    log(f"{label}: macro={macro:.4f}, weighted={weighted:.4f}, n={len(df_eval)}")
    return macro, weighted, len(df_eval)

# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m1.yaml")
    parser.add_argument("--mode", choices=["bootstrap", "retrain"], default="bootstrap")
    args = parser.parse_args()

    config = load_config(args.config)
    tracking_uri = setup_mlflow(config)

    train_path = os.environ.get("M1_TRAIN_PATH", config["train_path"])
    eval_path = os.environ.get("M1_EVAL_PATH", config["eval_path"])
    real_eval_path = os.environ.get("M1_REAL_EVAL_PATH", config["real_eval_path"])

    production_path = config.get("production_path") if args.mode == "retrain" else None

    registered_model_name = config.get("registered_model_name", "m1-categorization")

    log(f"Training mode={args.mode}")

    train_df = load_synthetic(train_path, production_path)
    eval_df = load_synthetic(eval_path, None)
    real_df = load_real_eval(real_eval_path)

    le = LabelEncoder()
    y_train = le.fit_transform(train_df[LABEL_COL])

    pipeline = build_model(config)

    with mlflow.start_run() as run:

        pipeline.fit(train_df, y_train)

        synth_macro, _, _ = evaluate(pipeline, eval_df, le, "synthetic_eval")
        real_macro, _, _ = evaluate(pipeline, real_df, le, "real_eval")

        mlflow.log_metric("synth_macro_f1", synth_macro)
        mlflow.log_metric("real_macro_f1", real_macro)

        prev_version, prev_metric = get_latest_production_metric(
            registered_model_name, "real_macro_f1"
        )

        do_register, reason = should_register(
            mode=args.mode,
            current_metric=real_macro,
            previous_metric=prev_metric,
            higher_is_better=True,
            absolute_gate_passed=True,
            prefix="M1",
        )

        mlflow.set_tag("register_reason", reason)

        mlflow.sklearn.log_model(pipeline, "model")

        if do_register:
            client = MlflowClient()
            mv = register_model_version(
                client,
                registered_model_name,
                run.info.run_id,
                "model"
            )
            log(f"REGISTERED v{mv.version}")
        else:
            log(f"NOT REGISTERED: {reason}")

        log("Training complete")

if __name__ == "__main__":
    main()