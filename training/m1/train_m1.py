import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import yaml
import argparse
import time
import platform
import os
import subprocess
import warnings
import logging
from pathlib import Path
from mlflow.tracking import MlflowClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.dummy import DummyClassifier
from sklearn.metrics import f1_score, classification_report
from sklearn.preprocessing import LabelEncoder
from scipy.sparse import hstack, csr_matrix
import xgboost as xgb

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
warnings.filterwarnings("ignore")
logging.getLogger("mlflow").setLevel(logging.ERROR)

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

CACHE_DIR = Path(os.environ.get("DATA_CACHE_DIR", "/tmp/nb_data_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def _resolve_path(path):
    """Return a local filesystem path. Downloads from Swift if needed and caches."""
    if not path.startswith("swift://"):
        return path
    _, rest = path.split("swift://", 1)
    container, object_name = rest.split("/", 1)
    local_path = CACHE_DIR / container / object_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if not local_path.exists():
        print(f"[M1] downloading {path} -> {local_path}")
        subprocess.run(
            ["swift", "download", container, object_name, "-o", str(local_path)],
            check=True,
        )
    return str(local_path)


def load_synthetic_split(path):
    local = _resolve_path(path)
    df = pd.read_csv(local)
    df = df.dropna(subset=[LABEL_COL, TEXT_COL])
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def load_real_eval(path):
    """Load real MoneyData and derive the numeric feature columns on the fly."""
    local = _resolve_path(path)
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
    model_type = config["model"]["type"]
    if model_type == "baseline_dummy":
        clf = DummyClassifier(strategy="most_frequent")
    elif model_type == "baseline_logreg":
        clf = LogisticRegression(max_iter=2000, C=1.0)
    elif model_type == "xgboost":
        clf = xgb.XGBClassifier(
            n_estimators=config["model"]["n_estimators"],
            max_depth=config["model"]["max_depth"],
            learning_rate=config["model"]["learning_rate"],
            eval_metric="mlogloss",
            tree_method="hist",
            n_jobs=-1,
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    return tfidf, clf


def featurize(tfidf, df, fit=False):
    text_vec = (
        tfidf.fit_transform(df[TEXT_COL].astype(str))
        if fit
        else tfidf.transform(df[TEXT_COL].astype(str))
    )
    num_cols_present = [c for c in NUMERIC_COLS if c in df.columns]
    if num_cols_present:
        num_vec = csr_matrix(df[num_cols_present].values.astype(float))
        return hstack([text_vec, num_vec])
    return text_vec


def evaluate(clf, tfidf, df, le, label):
    """Run eval on a dataframe; return (macro_f1, weighted_f1, filtered_size, n_classes_seen)."""
    known_mask = df[LABEL_COL].isin(le.classes_)
    n_filtered = int((~known_mask).sum())
    df_eval = df[known_mask].reset_index(drop=True)
    if len(df_eval) == 0:
        print(f"[M1] {label}: NO rows with known labels after filtering")
        return 0.0, 0.0, 0, 0
    X = featurize(tfidf, df_eval, fit=False)
    y_true = le.transform(df_eval[LABEL_COL])
    y_pred = clf.predict(X)
    macro = f1_score(y_true, y_pred, average="macro", zero_division=0)
    weighted = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    n_classes = len(set(y_true))
    print(
        f"[M1] {label}: macro_f1={macro:.4f} weighted_f1={weighted:.4f} "
        f"n={len(df_eval)} classes={n_classes} filtered={n_filtered}"
    )
    return macro, weighted, len(df_eval), n_classes


def best_registered_real_macro_f1(model_name):
    client = MlflowClient()
    best_metric = 0.0
    best_version = None
    try:
        existing = client.search_model_versions(f"name='{model_name}'")
    except Exception:
        return best_metric, best_version

    for version in existing:
        try:
            run = client.get_run(version.run_id)
        except Exception:
            continue
        previous = float(run.data.metrics.get("real_macro_f1", 0.0))
        if previous > best_metric:
            best_metric = previous
            best_version = version.version
    return best_metric, best_version


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config_m1.yaml")
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", config.get("mlflow_tracking_uri"))
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(config["experiment_name"])

    train_path = os.environ.get("M1_TRAIN_PATH", config["train_path"])
    synth_eval_path = os.environ.get("M1_EVAL_PATH", config["eval_path"])
    real_eval_path = os.environ.get("M1_REAL_EVAL_PATH", config["real_eval_path"])
    gate_floor = float(os.environ.get("M1_GATE_FLOOR", config.get("quality_gate_floor", 0.55)))
    gate_ceiling = float(
        os.environ.get("M1_GATE_CEILING", config.get("quality_gate_ceiling", 0.98))
    )
    improvement_threshold = float(
        os.environ.get(
            "M1_IMPROVEMENT_THRESHOLD", config.get("improvement_threshold", 0.005)
        )
    )
    registered_model_name = config.get("registered_model_name", "m1-categorization")

    print(f"[M1] tracking_uri={tracking_uri}")
    print(f"[M1] train_path={train_path}")
    print(f"[M1] synth_eval_path={synth_eval_path}")
    print(f"[M1] real_eval_path={real_eval_path}")
    print(f"[M1] gate floor={gate_floor} ceiling={gate_ceiling}")
    print(f"[M1] improvement_threshold={improvement_threshold}")

    train_df = load_synthetic_split(train_path)
    synth_eval_df = load_synthetic_split(synth_eval_path)
    real_eval_df = load_real_eval(real_eval_path)

    le = LabelEncoder()
    y_train = le.fit_transform(train_df[LABEL_COL])

    tfidf, clf = build_model(config)
    X_train = featurize(tfidf, train_df, fit=True)

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "model_type": config["model"]["type"],
                "tfidf_analyzer": config["tfidf"].get("analyzer", "word"),
                "tfidf_ngram_min": config["tfidf"]["ngram_min"],
                "tfidf_ngram_max": config["tfidf"]["ngram_max"],
                "tfidf_max_features": config["tfidf"]["max_features"],
                "train_size": len(train_df),
                "synth_eval_size": len(synth_eval_df),
                "real_eval_size": len(real_eval_df),
                "num_classes_trained": len(le.classes_),
                "train_path": train_path,
                "synth_eval_path": synth_eval_path,
                "real_eval_path": real_eval_path,
                "gate_floor": gate_floor,
                "gate_ceiling": gate_ceiling,
                "improvement_threshold": improvement_threshold,
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

        # Synthetic eval is informational only.
        synth_macro, synth_weighted, synth_n, synth_cls = evaluate(
            clf, tfidf, synth_eval_df, le, "synthetic_eval"
        )
        mlflow.log_metric("synth_macro_f1", synth_macro)
        mlflow.log_metric("synth_weighted_f1", synth_weighted)
        mlflow.log_metric("synth_eval_n", synth_n)

        # Real eval is the gated metric.
        real_macro, real_weighted, real_n, real_cls = evaluate(
            clf, tfidf, real_eval_df, le, "real_eval"
        )
        mlflow.log_metric("real_macro_f1", real_macro)
        mlflow.log_metric("real_weighted_f1", real_weighted)
        mlflow.log_metric("real_eval_n", real_n)
        mlflow.log_metric("real_eval_classes", real_cls)

        gate_passed = gate_floor <= real_macro <= gate_ceiling
        best_existing, best_existing_version = best_registered_real_macro_f1(
            registered_model_name
        )
        is_improved = real_macro > (best_existing + improvement_threshold)
        mlflow.log_metric("best_existing_real_macro_f1", best_existing)
        mlflow.log_metric("required_real_macro_f1", best_existing + improvement_threshold)
        mlflow.set_tag("quality_gate_metric", "real_macro_f1")
        mlflow.set_tag("quality_gate_passed", str(gate_passed).lower())
        mlflow.set_tag("quality_gate_floor", str(gate_floor))
        mlflow.set_tag("quality_gate_ceiling", str(gate_ceiling))
        mlflow.set_tag("improvement_threshold", str(improvement_threshold))
        mlflow.set_tag("best_existing_real_macro_f1", str(best_existing))
        if best_existing_version is not None:
            mlflow.set_tag("best_existing_model_version", str(best_existing_version))

        if gate_passed and is_improved:
            mlflow.sklearn.log_model(
                clf,
                artifact_path="model",
                registered_model_name=registered_model_name,
            )
            print(
                f"[M1] PASSED gate ({gate_floor} <= {real_macro:.4f} <= {gate_ceiling}) "
                f"and beat best existing ({best_existing:.4f}) by at least "
                f"{improvement_threshold:.4f} "
                f"registered as '{registered_model_name}'"
            )
        else:
            if not gate_passed:
                mlflow.set_tag("rejected_by_gate", "true")
            if gate_passed and not is_improved:
                mlflow.set_tag("no_improvement", "true")
            mlflow.sklearn.log_model(clf, artifact_path="model")
            if not gate_passed and real_macro > gate_ceiling:
                print(
                    f"[M1] FAILED gate suspiciously perfect "
                    f"({real_macro:.4f} > {gate_ceiling}). Possible data leakage. NOT registered."
                )
            elif not gate_passed:
                print(f"[M1] FAILED gate below floor ({real_macro:.4f} < {gate_floor}). NOT registered.")
            else:
                print(
                    f"[M1] FAILED registration no improvement "
                    f"({real_macro:.4f} <= {best_existing:.4f} + {improvement_threshold:.4f}). "
                    "Run logged, NOT registered."
                )

        print(
            f"[M1] done | synth_macro={synth_macro:.4f} real_macro={real_macro:.4f} "
            f"train_time={train_time:.1f}s run_id={run.info.run_id}"
        )


if __name__ == "__main__":
    main()
