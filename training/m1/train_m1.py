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
from sklearn.metrics import f1_score, log_loss, top_k_accuracy_score
from sklearn.pipeline import Pipeline
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


def ensure_numeric_cols(df):
    for col in NUMERIC_COLS:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = df[col].fillna(0)
    return df


def load_synthetic(bootstrap_path, production_path):
    df = load_and_combine(bootstrap_path, production_path, prefix="M1")
    df = df.dropna(subset=[LABEL_COL, TEXT_COL])
    return ensure_numeric_cols(df)


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
    return ensure_numeric_cols(df)


def split_feedback_for_eval(df, config):
    fraction = float(
        os.environ.get(
            "M1_FEEDBACK_EVAL_FRACTION",
            config.get("feedback_eval_fraction", 0.2),
        )
    )
    fraction = max(0.0, min(fraction, 0.5))
    if len(df) < 2 or fraction <= 0:
        return df.copy(), df.iloc[0:0].copy(), fraction

    df = df.sort_values("date").reset_index(drop=True)
    eval_rows = max(1, int(len(df) * fraction))
    eval_rows = min(eval_rows, len(df) - 1)
    train_feedback = df.iloc[:-eval_rows].copy()
    eval_feedback = df.iloc[-eval_rows:].copy()
    return train_feedback, eval_feedback, fraction


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
    return Pipeline(
        [
            (
                "features",
                ColumnTransformer(
                    [
                        ("text", tfidf, TEXT_COL),
                        ("num", "passthrough", NUMERIC_COLS),
                    ],
                    sparse_threshold=1.0,
                ),
            ),
            ("clf", clf),
        ]
    )


def evaluate(pipeline, df, le, label):
    known_mask = df[LABEL_COL].isin(le.classes_)
    n_filtered = int((~known_mask).sum())
    df_eval = df[known_mask].reset_index(drop=True)
    if len(df_eval) == 0:
        return {
            "macro_f1": 0.0,
            "weighted_f1": 0.0,
            "top3_accuracy": 0.0,
            "log_loss": 0.0,
            "n_rows": 0,
        }
    y_true = le.transform(df_eval[LABEL_COL])
    y_pred = pipeline.predict(df_eval)
    y_proba = pipeline.predict_proba(df_eval)
    macro = f1_score(y_true, y_pred, average="macro", zero_division=0)
    weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    top3 = top_k_accuracy_score(y_true, y_proba, k=min(3, len(le.classes_)), labels=np.arange(len(le.classes_)))
    loss = log_loss(y_true, y_proba, labels=np.arange(len(le.classes_)))
    log(
        f"{label}: macro_f1={macro:.4f} weighted_f1={weighted:.4f} "
        f"top3_accuracy={top3:.4f} log_loss={loss:.4f} n={len(df_eval)} filtered={n_filtered}"
    )
    return {
        "macro_f1": macro,
        "weighted_f1": weighted,
        "top3_accuracy": top3,
        "log_loss": loss,
        "n_rows": len(df_eval),
    }


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
    gate_floor = float(
        os.environ.get(
            "M1_GATE_TOP3_FLOOR",
            os.environ.get(
                "M1_GATE_FLOOR",
                config.get("quality_gate_top3_floor", config.get("quality_gate_floor", 0.55)),
            ),
        )
    )
    gate_ceiling = float(
        os.environ.get(
            "M1_GATE_TOP3_CEILING",
            os.environ.get(
                "M1_GATE_CEILING",
                config.get("quality_gate_top3_ceiling", config.get("quality_gate_ceiling", 0.98)),
            ),
        )
    )
    registered_model_name = config.get("registered_model_name", "m1-categorization")

    log(f"mode={args.mode} tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"production_path={production_path}")
    log(f"gate floor={gate_floor} ceiling={gate_ceiling}")

    train_df = load_synthetic(train_path, None)
    synth_eval_df = load_synthetic(synth_eval_path, None)
    real_eval_df = load_real_eval(real_eval_path)
    production_rows = 0
    production_train_rows = 0
    production_eval_rows = 0
    feedback_eval_fraction = 0.0
    if production_path:
        production_df = load_real_eval(production_path)
        production_rows = len(production_df)
        if production_rows > 0:
            train_feedback_df, eval_feedback_df, feedback_eval_fraction = split_feedback_for_eval(
                production_df, config
            )
            production_train_rows = len(train_feedback_df)
            production_eval_rows = len(eval_feedback_df)
            if production_train_rows > 0:
                train_df = pd.concat([train_df, train_feedback_df], ignore_index=True)
            if production_eval_rows > 0:
                real_eval_df = pd.concat([real_eval_df, eval_feedback_df], ignore_index=True)
            log(
                f"loaded {production_rows} production rows "
                f"(train={production_train_rows}, real_eval={production_eval_rows}) "
                f"from {production_path}"
            )

    le = LabelEncoder()
    y_train = le.fit_transform(train_df[LABEL_COL])

    pipeline = build_model(config)

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
                "production_rows": production_rows,
                "production_train_rows": production_train_rows,
                "production_eval_rows": production_eval_rows,
                "feedback_eval_fraction": feedback_eval_fraction,
                "gate_top3_floor": gate_floor,
                "gate_top3_ceiling": gate_ceiling,
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
        pipeline.fit(train_df, y_train)
        train_time = time.time() - start
        mlflow.log_metric("train_time_seconds", train_time)

        synth_metrics = evaluate(pipeline, synth_eval_df, le, "synthetic_eval")
        real_metrics = evaluate(pipeline, real_eval_df, le, "real_eval")

        mlflow.log_metric("synth_macro_f1", synth_metrics["macro_f1"])
        mlflow.log_metric("synth_weighted_f1", synth_metrics["weighted_f1"])
        mlflow.log_metric("synth_top3_accuracy", synth_metrics["top3_accuracy"])
        mlflow.log_metric("synth_log_loss", synth_metrics["log_loss"])
        mlflow.log_metric("real_macro_f1", real_metrics["macro_f1"])
        mlflow.log_metric("real_weighted_f1", real_metrics["weighted_f1"])
        mlflow.log_metric("real_top3_accuracy", real_metrics["top3_accuracy"])
        mlflow.log_metric("real_log_loss", real_metrics["log_loss"])

        absolute_passed = gate_floor <= real_metrics["top3_accuracy"] <= gate_ceiling
        prev_version, prev_metric = get_latest_production_metric(registered_model_name, "real_top3_accuracy")
        log(f"previous version: v{prev_version} real_top3_accuracy={prev_metric}")

        do_register, reason = should_register(
            mode=args.mode,
            current_metric=real_metrics["top3_accuracy"],
            previous_metric=prev_metric,
            higher_is_better=True,
            absolute_gate_passed=absolute_passed,
            prefix="M1",
        )

        mlflow.set_tag("quality_gate_metric", "real_top3_accuracy")
        mlflow.set_tag("absolute_gate_passed", str(absolute_passed).lower())
        mlflow.set_tag("registered", str(do_register).lower())
        mlflow.set_tag("register_reason", reason)
        mlflow.set_tag("previous_version", str(prev_version) if prev_version else "none")
        mlflow.set_tag("mode", args.mode)

        with tempfile.TemporaryDirectory() as tmpdir:
            le_path = Path(tmpdir) / "label_encoder.joblib"
            joblib.dump(le, le_path)
            mlflow.log_artifact(str(le_path), artifact_path="model")
            tfidf_path = Path(tmpdir) / "tfidf_vectorizer.joblib"
            tfidf = pipeline.named_steps["features"].named_transformers_["text"]
            joblib.dump(tfidf, tfidf_path)
            mlflow.log_artifact(str(tfidf_path), artifact_path="preprocessor")

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
