import argparse
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


TEXT_COL = "merchant"
TARGET_COL = "project_category"

NUMERIC_FEATURES = [
    "abs_amount",
    "log_abs_amount",
    "day_of_week",
    "day_of_month",
    "month",
    "repeat_count",
    "is_recurring_candidate",
]

CATEGORICAL_FEATURES = [
    "transaction_type",
    "persona_cluster",
]


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    needed = [TEXT_COL, TARGET_COL] + NUMERIC_FEATURES + CATEGORICAL_FEATURES
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df.dropna(subset=[TEXT_COL, TARGET_COL]).copy()
    df[TEXT_COL] = df[TEXT_COL].astype(str).str.upper().str.strip()

    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    for col in CATEGORICAL_FEATURES:
        df[col] = df[col].astype(str).fillna("unknown")

    return df


def build_model() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "merchant_tfidf",
                TfidfVectorizer(
                    analyzer="char_wb",
                    ngram_range=(3, 5),
                    min_df=2,
                    max_features=2000,
                ),
                TEXT_COL,
            ),
            (
                "numeric",
                StandardScaler(),
                NUMERIC_FEATURES,
            ),
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore"),
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    classifier = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        n_jobs=-1,
        multi_class="auto",
    )

    return Pipeline(
        steps=[
            ("features", preprocessor),
            ("classifier", classifier),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--out", default="training/m1/autocategorization_model.joblib")
    parser.add_argument("--mlflow-uri", default=None)
    parser.add_argument("--experiment-name", default="m1-autocategorization-logreg")
    args = parser.parse_args()

    if args.mlflow_uri:
        mlflow.set_tracking_uri(args.mlflow_uri)
    mlflow.set_experiment(args.experiment_name)

    df = load_data(args.data)

    X = df[[TEXT_COL] + NUMERIC_FEATURES + CATEGORICAL_FEATURES]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = build_model()

    with mlflow.start_run():
        mlflow.log_params(
            {
                "text_col": TEXT_COL,
                "target_col": TARGET_COL,
                "numeric_features": ",".join(NUMERIC_FEATURES),
                "categorical_features": ",".join(CATEGORICAL_FEATURES),
                "tfidf_analyzer": "char_wb",
                "tfidf_ngram_range": "3,5",
                "tfidf_min_df": 2,
                "tfidf_max_features": 2000,
                "classifier": "LogisticRegression",
                "class_weight": "balanced",
                "max_iter": 2000,
                "data_path": args.data,
                "train_rows": len(X_train),
                "test_rows": len(X_test),
                "num_classes": y.nunique(),
            }
        )

        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        accuracy = accuracy_score(y_test, preds)
        macro_f1 = f1_score(y_test, preds, average="macro")
        weighted_f1 = f1_score(y_test, preds, average="weighted")
        report = classification_report(y_test, preds, output_dict=True, zero_division=0)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("macro_f1", macro_f1)
        mlflow.log_metric("weighted_f1", weighted_f1)

        for label, metrics in report.items():
            if isinstance(metrics, dict) and "f1-score" in metrics:
                safe = str(label).replace(" ", "_").replace("/", "_")
                mlflow.log_metric(f"f1_{safe}", metrics["f1-score"])

        bundle = {
            "model": model,
            "target_col": TARGET_COL,
            "text_col": TEXT_COL,
            "numeric_features": NUMERIC_FEATURES,
            "categorical_features": CATEGORICAL_FEATURES,
            "classes": list(model.named_steps["classifier"].classes_),
        }

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(bundle, out_path)

        mlflow.log_artifact(str(out_path))
        mlflow.sklearn.log_model(model, artifact_path="model")

        print("Accuracy:", round(accuracy, 4))
        print("Macro F1:", round(macro_f1, 4))
        print()
        print(classification_report(y_test, preds, zero_division=0))
        print(f"Saved model bundle to {out_path}")


if __name__ == "__main__":
    main()
