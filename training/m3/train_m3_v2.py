import time
from pathlib import Path
import numpy as np

import joblib
import mlflow
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

DATA_PATH = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_v2_train.csv"
EVAL_PATH = REPO_ROOT / "data_pipeline" / "processed" / "batch_datasets" / "forecasting_v2_eval.csv"

BUNDLE_PATH = BASE_DIR / "m3_v2_bundle.joblib"
LATEST_FEATURES_PATH = BASE_DIR / "m3_v2_latest_features.csv"
MODEL_PATH = BASE_DIR / "m3_v2_model.pkl"


def load_data(path: Path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["year_month"]).dt.month
    df["year"] = pd.to_datetime(df["year_month"]).dt.year

    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    drop_cols = [
        "synthetic_user_id",
        "year_month",
    ]
    target = "target_next_month_spend"

    X = df.drop(columns=drop_cols + [target])
    y = df[target]

    return X, y, df


def evaluate_per_category(df, y_true, y_pred):
    df = df.copy()
    df["y_true"] = y_true
    df["y_pred"] = y_pred
    df["abs_err"] = (df["y_true"] - df["y_pred"]).abs()

    per_cat = (
        df.groupby("project_category")["abs_err"]
        .mean()
        .reset_index(name="mae")
    )
    return per_cat


def main():
    mlflow.set_experiment("m3-forecast-v2")

    X_train, y_train, train_df = load_data(DATA_PATH)
    X_eval, y_eval, eval_df = load_data(EVAL_PATH)

    # One-hot encode any categorical columns consistently
    X_train = pd.get_dummies(X_train, dummy_na=True)
    X_eval = pd.get_dummies(X_eval, dummy_na=True)
    X_eval = X_eval.reindex(columns=X_train.columns, fill_value=0)

    with mlflow.start_run():
        start = time.time()

        model = HistGradientBoostingRegressor(
            max_depth=6,
            learning_rate=0.05,
            max_iter=300,
            random_state=42,
        )
        model.fit(X_train, y_train)

        train_time = time.time() - start

        # Predictions
        y_pred = model.predict(X_eval)
        overall_mae = mean_absolute_error(y_eval, y_pred)

        per_cat = evaluate_per_category(eval_df, y_eval, y_pred)
        median_per_cat_mae = float(per_cat["mae"].median())

        # Save reusable inference bundle
        bundle = {
            "model": model,
            "feature_columns": list(X_train.columns),
            "model_name": "m3-forecast-v2",
        }
        joblib.dump(bundle, BUNDLE_PATH)

        # Save latest feature rows for inference
        full_df = pd.concat([train_df, eval_df], ignore_index=True)
        latest_feature_rows = (
            full_df.sort_values(["synthetic_user_id", "project_category", "year_month"])
            .groupby(["synthetic_user_id", "project_category"], as_index=False)
            .tail(1)
            .copy()
        )
        latest_feature_rows.to_csv(LATEST_FEATURES_PATH, index=False)

        # Optional raw model artifact
        joblib.dump(model, MODEL_PATH)

        # MLflow logging
        mlflow.log_param("model", "HistGradientBoostingRegressor")
        mlflow.log_param("features", X_train.shape[1])
        mlflow.log_param("train_rows", len(train_df))
        mlflow.log_param("eval_rows", len(eval_df))

        mlflow.log_metric("overall_mae", overall_mae)
        mlflow.log_metric("median_per_category_mae", median_per_cat_mae)
        mlflow.log_metric("train_time_seconds", train_time)

        for _, row in per_cat.iterrows():
            name = row["project_category"].replace(" ", "_").replace("/", "_")
            mlflow.log_metric(f"mae_{name}", float(row["mae"]))

        mlflow.log_artifact(str(BUNDLE_PATH))
        mlflow.log_artifact(str(LATEST_FEATURES_PATH))
        mlflow.log_artifact(str(MODEL_PATH))

        print("Training complete")
        print(f"Overall MAE: {overall_mae:.4f}")
        print(f"Median per-category MAE: {median_per_cat_mae:.4f}")
        print(f"Saved bundle: {BUNDLE_PATH}")
        print(f"Saved latest features: {LATEST_FEATURES_PATH}")


if __name__ == "__main__":
    main()