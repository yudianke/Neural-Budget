import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import joblib
import mlflow
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.tracking import MlflowClient
from scipy.sparse import csr_matrix, hstack

import mock_model
from schemas import CategorySuggestion, M1FeedbackEntry, M1Input, M1Output

TEXT_COL = "merchant"
NUMERIC_COLS = ["log_amount", "day_of_week", "day_of_month"]

# Maps Ray model output labels → project dataset categories (moneydata_labeled.csv)
CATEGORY_MAP: dict[str, str] = {
    # Food & Drink
    "Dine Out": "restaurants",
    "Groceries": "groceries",
    "Groceries ": "groceries",          # trailing-space variant in training data
    # Shopping
    "Amazon": "shopping",
    "Clothes": "shopping",
    "Other Shopping": "shopping",
    "Home Improvement": "shopping",
    "Services/Home Improvement": "shopping",
    # Housing & Bills
    "Mortgage": "housing",
    "Rent": "housing",
    "Bills": "utilities",
    "Insurance": "utilities",
    # Transport
    "Travel": "transport",
    # Health
    "Health": "healthcare",
    "Fitness": "healthcare",
    # Entertainment
    "Entertainment": "entertainment",
    "Hotels": "entertainment",
    # Personal Care / Services
    "Services": "personal_care",
    # Finance / Cash
    "Account transfer": "cash_transfers",
    "Cash": "cash_transfers",
    "Savings": "cash_transfers",
    "Investment": "cash_transfers",
    "Paycheck": "cash_transfers",
    "Supplementary Income": "cash_transfers",
    "Interest": "cash_transfers",
    # Catch-all (literal garbage label from training data)
    "Purchase of uk.eg.org": "misc",
    "Others": "misc",
}

DEFAULT_TRACKING_URI = "http://129.114.27.211:8000"
# Try Ray model first, fall back to original sklearn pipeline
DEFAULT_MODEL_NAME = "m1-ray-categorization"
FALLBACK_MODEL_NAME = "m1-categorization"
DEFAULT_FEEDBACK_LOG_PATH = "/tmp/m1_ray_feedback.jsonl"

_booster = None
_label_encoder = None
_tfidf = None
_metadata = {}
_model_version = None
# fallback sklearn pipeline
_pipeline = None
_use_fallback = False
_fallback_mode = None


def map_category(label: str) -> str:
    """Translate a Ray model output label to a project dataset category."""
    return CATEGORY_MAP.get(label, CATEGORY_MAP.get(label.strip(), "misc"))


def normalize_merchant(name: str) -> str:
    if not isinstance(name, str):
        return ""
    value = name.upper().strip()
    value = re.sub(r"\b\d{4,}\b", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _feedback_log_path() -> Path:
    return Path(os.environ.get("M1_FEEDBACK_LOG_PATH", DEFAULT_FEEDBACK_LOG_PATH))


def _select_model_version(client: MlflowClient, model_name: str):
    requested_version = os.environ.get("M1_MODEL_VERSION")
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No registered versions found for model '{model_name}'")

    versions = sorted(versions, key=lambda version: int(version.version), reverse=True)
    if requested_version:
        for version in versions:
            if str(version.version) == str(requested_version):
                return version
        raise RuntimeError(f"Requested model version {requested_version} not found for '{model_name}'")
    return versions[0]


def _tracking_reachable(tracking_uri: str) -> bool:
    try:
        with urlopen(tracking_uri, timeout=2):
            return True
    except (URLError, TimeoutError, OSError):
        return False


def load():
    global _booster, _label_encoder, _tfidf, _metadata, _model_version, _pipeline, _use_fallback, _fallback_mode

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
    model_name = os.environ.get("M1_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME)
    if not _tracking_reachable(tracking_uri):
        _use_fallback = True
        _fallback_mode = "mock"
        _model_version = "mock"
        _pipeline = None
        _booster = None
        _tfidf = None
        _label_encoder = None
        print(f"[M1] Tracking host {tracking_uri} unreachable, using local mock fallback")
        return None, None

    mlflow.set_tracking_uri(tracking_uri)

    # Try loading the Ray XGBoost bundle first
    try:
        client = MlflowClient(tracking_uri=tracking_uri)
        version = _select_model_version(client, model_name)
        bundle_dir = Path(
            mlflow.artifacts.download_artifacts(run_id=version.run_id, artifact_path="bundle")
        )
        metadata_path = bundle_dir / "metadata.json"
        tfidf_path = bundle_dir / "tfidf_vectorizer.joblib"
        label_encoder_path = bundle_dir / "label_encoder.joblib"
        model_path = bundle_dir / "model.ubj"
        if not model_path.exists():
            model_path = bundle_dir / "model.json"

        with metadata_path.open() as fh:
            _metadata = json.load(fh)
        _tfidf = joblib.load(tfidf_path)
        _label_encoder = joblib.load(label_encoder_path)
        _booster = xgb.Booster()
        _booster.load_model(str(model_path))
        _model_version = str(version.version)
        _use_fallback = False
        _fallback_mode = None
        print(f"[M1] Loaded Ray model '{model_name}' version {_model_version}")
        return _booster, _label_encoder

    except Exception as e:
        print(f"[M1] Ray model load failed ({e}), trying fallback sklearn pipeline...")

    # Fallback: load original sklearn pipeline (m1-categorization)
    import mlflow.sklearn as mlflow_sklearn
    try:
        _pipeline = mlflow_sklearn.load_model(f"models:/{FALLBACK_MODEL_NAME}/latest")
    except Exception:
        _pipeline = mlflow_sklearn.load_model("models:/m1-categorization/7")

    # Build label encoder from pipeline classes
    from sklearn.preprocessing import LabelEncoder
    _label_encoder = LabelEncoder()
    _label_encoder.classes_ = _pipeline.classes_
    _use_fallback = True
    _fallback_mode = "sklearn"
    _model_version = "fallback-sklearn"
    print(f"[M1] Loaded fallback sklearn pipeline '{FALLBACK_MODEL_NAME}'")
    return _pipeline, _label_encoder


def get_model_info():
    return {
        "model_name": os.environ.get("M1_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME),
        "model_version": _model_version,
        "class_count": len(_label_encoder.classes_) if _label_encoder is not None else 0,
        "mode": _fallback_mode if _use_fallback else "ray-xgboost",
    }


def _build_row(x: M1Input) -> tuple[pd.DataFrame, str]:
    merchant_clean = normalize_merchant(x.merchant)
    row = pd.DataFrame(
        [
            {
                TEXT_COL: merchant_clean,
                "log_amount": float(x.log_abs_amount),
                "day_of_week": int(x.day_of_week),
                "day_of_month": int(x.day_of_month),
            }
        ]
    )
    return row, merchant_clean


def _predict_proba(row: pd.DataFrame) -> np.ndarray:
    text_vec = _tfidf.transform(row[TEXT_COL].astype(str))
    num_vec = csr_matrix(row[NUMERIC_COLS].values.astype(float))
    features = hstack([text_vec, num_vec]).toarray()
    feature_names = _metadata.get("feature_columns")
    dmatrix = xgb.DMatrix(features, feature_names=feature_names)
    pred = _booster.predict(dmatrix)
    if pred.ndim == 1:
        pred = pred.reshape(-1, 1)
    return pred[0]


def predict(x: M1Input) -> M1Output:
    if _use_fallback and _fallback_mode == "mock":
        return mock_model.predict_category(x)

    if _label_encoder is None:
        raise RuntimeError("Model not loaded. Call real_model.load() at startup.")

    # Fallback path: use sklearn pipeline directly
    if _use_fallback:
        row, _ = _build_row(x)
        proba = _pipeline.predict_proba(row)[0]
        classes = _pipeline.classes_
        order = proba.argsort()[::-1]
        top3 = [
            CategorySuggestion(
                category=map_category(str(classes[i])),
                confidence=float(proba[i]),
            )
            for i in order[:3]
        ]
        predicted = top3[0].category
        confidence = top3[0].confidence
        auto_fill = confidence >= 0.6
        return M1Output(
            transaction_id=x.transaction_id,
            synthetic_user_id=x.synthetic_user_id,
            predicted_category=predicted,
            confidence=confidence,
            top_3_suggestions=top3,
        )

    if _booster is None or _tfidf is None:
        raise RuntimeError("Ray model not loaded.")

    row, _merchant_clean = _build_row(x)
    pred = _predict_proba(row)

    if pred.shape[0] == len(_label_encoder.classes_):
        proba = pred.astype(float)
        order = np.argsort(proba)[::-1]
    else:
        predicted_idx = int(pred[0])
        proba = np.zeros(len(_label_encoder.classes_), dtype=float)
        proba[predicted_idx] = 1.0
        order = np.array([predicted_idx])

    top_indices = order[: min(3, len(order))]
    top3 = [
        CategorySuggestion(
            category=map_category(str(_label_encoder.inverse_transform([int(idx)])[0])),
            confidence=float(proba[int(idx)]),
        )
        for idx in top_indices
    ]
    predicted = top3[0].category
    confidence = top3[0].confidence

    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category=predicted,
        confidence=confidence,
        top_3_suggestions=top3,
    )


def log_feedback(entries: list[M1FeedbackEntry]) -> int:
    path = _feedback_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with path.open("a", encoding="utf-8") as fh:
        for entry in entries:
            payload = entry.model_dump()
            if not payload.get("logged_at"):
                payload["logged_at"] = datetime.now(timezone.utc).isoformat()
            if not payload.get("model_name"):
                payload["model_name"] = os.environ.get(
                    "M1_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME
                )
            if not payload.get("model_version"):
                payload["model_version"] = _model_version
            fh.write(json.dumps(payload) + "\n")
            count += 1
    return count
