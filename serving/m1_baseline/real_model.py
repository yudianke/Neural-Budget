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

from schemas import CategorySuggestion, M1FeedbackEntry, M1Input, M1Output

TEXT_COL = "merchant"
NUMERIC_COLS = ["log_amount", "day_of_week", "day_of_month"]
CONFIDENCE_THRESHOLD = 0.6  # auto_fill if confidence >= this value

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
    """Load the Ray XGBoost bundle from MLflow.

    Fail-open policy:
      - If MLflow is unreachable or model load fails, the service stays up
        but enters 'degraded' mode: predictions return null/zero rather than
        serving garbage from a mock or schema-divergent fallback model.
      - /health reports {"status": "degraded"} so monitoring can alert.
      - No sklearn fallback, no mock fallback — wrong predictions are worse
        than no predictions for feedback quality and retraining integrity.
    """
    global _booster, _label_encoder, _tfidf, _metadata, _model_version, _pipeline, _use_fallback, _fallback_mode

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
    model_name = os.environ.get("M1_REGISTERED_MODEL_NAME", DEFAULT_MODEL_NAME)

    if not _tracking_reachable(tracking_uri):
        _use_fallback = True
        _fallback_mode = "degraded"
        _model_version = None
        _booster = None
        _tfidf = None
        _label_encoder = None
        _pipeline = None
        print(f"[M1] WARNING: MLflow at {tracking_uri} unreachable — entering degraded mode (no predictions)")
        return None, None

    mlflow.set_tracking_uri(tracking_uri)

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

        # Ray Train's XGBoostTrainer saves the model with multi:softmax regardless
        # of the objective passed in params. Re-set to multi:softprob so predict()
        # returns a full [n_samples, n_classes] probability matrix instead of a
        # 1D class-index array. The underlying weights are identical — only the
        # output interpretation changes.
        _booster.set_param("objective", "multi:softprob")
        _booster.set_param("num_class", str(len(_label_encoder.classes_)))

        _model_version = str(version.version)
        _use_fallback = False
        _fallback_mode = None
        print(f"[M1] Loaded Ray model '{model_name}' version {_model_version} (softprob mode)")
        return _booster, _label_encoder

    except Exception as e:
        # Model load failed — enter degraded mode rather than serving a
        # schema-divergent fallback that would contaminate the feedback log.
        _use_fallback = True
        _fallback_mode = "degraded"
        _model_version = None
        _booster = None
        _tfidf = None
        _label_encoder = None
        _pipeline = None
        print(f"[M1] WARNING: Model load failed ({e}) — entering degraded mode (no predictions)")
        return None, None


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
    """Return a probability vector over all classes for a single input row.

    With multi:softprob (set in load()), XGBoost returns shape [1, n_classes]
    — a proper probability distribution that sums to 1.
    """
    text_vec = _tfidf.transform(row[TEXT_COL].astype(str))
    num_vec = csr_matrix(row[NUMERIC_COLS].values.astype(float))
    features = hstack([text_vec, num_vec]).toarray()
    feature_names = _metadata.get("feature_columns")
    dmatrix = xgb.DMatrix(features, feature_names=feature_names)
    pred = _booster.predict(dmatrix)

    if pred.ndim == 2:
        return pred[0]   # [n_classes] — standard softprob output
    return pred          # defensive: already [n_classes]


def _null_prediction(x: M1Input) -> M1Output:
    """Return a no-prediction response when the model is in degraded mode.
    Callers should check confidence == 0 to detect this case.
    """
    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category="",
        confidence=0.0,
        top_3_suggestions=[],
        auto_fill=False,
    )


def predict(x: M1Input) -> M1Output:
    # Degraded mode — return null prediction rather than serving garbage.
    # Service stays up; /health reports "degraded".
    if _use_fallback and _fallback_mode == "degraded":
        return _null_prediction(x)

    if _booster is None or _tfidf is None or _label_encoder is None:
        # Should not happen if load() was called at startup, but guard defensively.
        return _null_prediction(x)

    row, _merchant_clean = _build_row(x)
    proba = _predict_proba(row).astype(float)  # always [n_classes] with softprob
    order = np.argsort(proba)[::-1]

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
    auto_fill = confidence >= CONFIDENCE_THRESHOLD

    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category=predicted,
        confidence=confidence,
        top_3_suggestions=top3,
        auto_fill=auto_fill,
    )


def reload() -> None:
    """Hot-reload the model from MLflow. Called by retrain-daemon after new version registered."""
    global _booster, _label_encoder, _tfidf, _metadata, _model_version, _pipeline, _use_fallback, _fallback_mode
    print("[M1] reload() triggered — re-loading model from MLflow")
    load()
    print(f"[M1] reload() complete — now on version {_model_version}")


def get_feedback_stats(since_version: str | None = None) -> dict:
    """Return correction count, total feedback, correction rate.

    If since_version is given, only counts entries where model_version == since_version.
    Used by retrain-daemon to decide trigger and evaluate rollback.
    """
    path = _feedback_log_path()
    if not path.exists():
        return {
            "total": 0,
            "corrections": 0,
            "correction_rate": 0.0,
            "current_version": _model_version,
            "filter_version": since_version,
        }

    total, corrections = 0, 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if since_version and row.get("model_version") != since_version:
                continue
            total += 1
            if row.get("feedback_type") == "overridden":
                corrections += 1

    rate = corrections / total if total > 0 else 0.0
    return {
        "total": total,
        "corrections": corrections,
        "correction_rate": round(rate, 4),
        "current_version": _model_version,
        "filter_version": since_version,
    }


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
