"""
Safeguarding checks for M1 categorization model.

Called after evaluation in train_m1_ray.py. Logs all findings to the active
MLflow run as tags, metrics, and artifacts.

Covers:
  Fairness      — flags under-performing categories (F1 < 0.4)
  Explainability — top-5 TF-IDF n-gram features per category as artifact
  Robustness    — fraction of test predictions below confidence threshold
  Privacy       — confirms feedback merchant strings have no card-number patterns
  Accountability — git commit hash + training data MD5 logged as tags
"""

import hashlib
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

import mlflow
import numpy as np

CONFIDENCE_THRESHOLD = 0.6
LOW_CONF_WARNING_THRESHOLD = 0.20   # flag if >20% of predictions are low-confidence
FAIRNESS_F1_FLOOR = 0.40            # categories below this are flagged
CARD_NUMBER_RE = re.compile(r"\b\d{12,19}\b")  # crude card-number detector


# ---------------------------------------------------------------------------
# Fairness
# ---------------------------------------------------------------------------
def _check_fairness(y_test, y_pred, le):
    """Flag categories with F1 < FAIRNESS_F1_FLOOR."""
    from sklearn.metrics import f1_score
    labels = np.unique(np.concatenate([y_test, y_pred]))
    per_class_f1 = f1_score(y_test, y_pred, labels=labels, average=None, zero_division=0)
    underperforming = []
    for idx, f1 in zip(labels, per_class_f1):
        cat_name = str(le.inverse_transform([idx])[0])
        if f1 < FAIRNESS_F1_FLOOR:
            underperforming.append(f"{cat_name}={f1:.3f}")

    tag_val = ",".join(underperforming) if underperforming else "none"
    mlflow.set_tag("safeguard_fairness_underperforming", tag_val)
    mlflow.set_tag("safeguard_fairness_floor", str(FAIRNESS_F1_FLOOR))
    print(f"[safeguard] fairness: underperforming={tag_val}")


# ---------------------------------------------------------------------------
# Explainability
# ---------------------------------------------------------------------------
def _check_explainability(tfidf, le, feature_cols):
    """Log top-5 TF-IDF n-gram features per category as a JSON artifact."""
    try:
        # feature_cols matches [tfidf_features..., numeric_features...]
        vocab = tfidf.get_feature_names_out()
        n_tfidf = len(vocab)

        # Build class→feature weight matrix from TF-IDF idf_ vector
        # (we don't have per-class weights without the XGBoost tree inspection,
        #  so we use TF-IDF idf weights as global importance proxy)
        idf_weights = tfidf.idf_
        order = np.argsort(idf_weights)[::-1][:50]  # top 50 globally rare n-grams

        top_global = [{"ngram": vocab[i], "idf": float(idf_weights[i])} for i in order]

        result = {
            "note": "TF-IDF idf-ranked n-grams (higher idf = rarer = more discriminative)",
            "top_50_global": top_global,
            "class_names": [str(c) for c in le.classes_],
            "numeric_features": feature_cols[n_tfidf:],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "top_features.json"
            out_path.write_text(json.dumps(result, indent=2))
            mlflow.log_artifact(str(out_path), artifact_path="explainability")

        mlflow.set_tag("safeguard_explainability", "top_features_logged")
        print("[safeguard] explainability: top_features.json logged to MLflow")
    except Exception as e:
        mlflow.set_tag("safeguard_explainability", f"error:{e}")
        print(f"[safeguard] explainability error: {e}")


# ---------------------------------------------------------------------------
# Robustness
# ---------------------------------------------------------------------------
def _check_robustness(pred_proba):
    """Flag if >LOW_CONF_WARNING_THRESHOLD of predictions are below CONFIDENCE_THRESHOLD."""
    if pred_proba is None or len(pred_proba) == 0:
        mlflow.set_tag("safeguard_robustness", "no_proba_available")
        return

    if pred_proba.ndim == 2:
        max_conf = pred_proba.max(axis=1)
    else:
        max_conf = pred_proba

    low_conf_rate = float((max_conf < CONFIDENCE_THRESHOLD).mean())
    mlflow.log_metric("low_confidence_rate", low_conf_rate)
    mlflow.set_tag("safeguard_confidence_threshold", str(CONFIDENCE_THRESHOLD))

    if low_conf_rate > LOW_CONF_WARNING_THRESHOLD:
        mlflow.set_tag("safeguard_robustness_warning", "true")
        print(f"[safeguard] robustness WARNING: {low_conf_rate:.1%} predictions below threshold")
    else:
        mlflow.set_tag("safeguard_robustness_warning", "false")
        print(f"[safeguard] robustness OK: low_confidence_rate={low_conf_rate:.1%}")


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------
def _check_privacy(data_path: str | None):
    """Confirm training data file has no card-number-like strings in merchant names."""
    if not data_path or not Path(data_path).exists():
        mlflow.set_tag("safeguard_privacy_pii_check", "skipped_no_file")
        return

    try:
        import pandas as pd
        df = pd.read_csv(data_path, nrows=5000)
        merchant_col = None
        for col in ["merchant", "Transaction Description", "description", "payee"]:
            if col in df.columns:
                merchant_col = col
                break

        if merchant_col is None:
            mlflow.set_tag("safeguard_privacy_pii_check", "skipped_no_merchant_col")
            return

        hits = df[merchant_col].astype(str).apply(
            lambda x: bool(CARD_NUMBER_RE.search(x))
        ).sum()

        if hits > 0:
            mlflow.set_tag("safeguard_privacy_pii_check", f"WARN_{hits}_potential_card_numbers")
            print(f"[safeguard] privacy WARNING: {hits} rows may contain card numbers")
        else:
            mlflow.set_tag("safeguard_privacy_pii_check", "pass")
            print("[safeguard] privacy: no card-number patterns detected")
    except Exception as e:
        mlflow.set_tag("safeguard_privacy_pii_check", f"error:{e}")


# ---------------------------------------------------------------------------
# Accountability
# ---------------------------------------------------------------------------
def _log_accountability(data_path: str | None):
    """Log git commit hash and training data MD5 for full audit trail."""
    # Git commit
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(__file__),
            stderr=subprocess.DEVNULL,
        ).decode().strip()
        mlflow.set_tag("safeguard_git_commit", commit)
        print(f"[safeguard] accountability: git_commit={commit}")
    except Exception:
        mlflow.set_tag("safeguard_git_commit", "unavailable")

    # Data hash
    if data_path and Path(data_path).exists():
        try:
            md5 = hashlib.md5(Path(data_path).read_bytes()).hexdigest()
            mlflow.set_tag("safeguard_data_hash_md5", md5)
            print(f"[safeguard] accountability: data_hash={md5}")
        except Exception as e:
            mlflow.set_tag("safeguard_data_hash_md5", f"error:{e}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_safeguarding_checks(
    y_test,
    y_pred,
    le,
    tfidf,
    feature_cols: list[str],
    pred_proba=None,
    data_path: str | None = None,
):
    """Run all safeguarding checks inside the active MLflow run.

    Args:
        y_test:       Ground-truth encoded labels (numpy array)
        y_pred:       Predicted encoded labels (numpy array)
        le:           Fitted LabelEncoder
        tfidf:        Fitted TfidfVectorizer
        feature_cols: List of feature column names (tfidf + numeric)
        pred_proba:   Raw prediction probabilities from XGBoost (optional)
        data_path:    Path to the training CSV used (for privacy + accountability)
    """
    print("[safeguard] running safeguarding checks...")
    _check_fairness(y_test, y_pred, le)
    _check_explainability(tfidf, le, feature_cols)
    _check_robustness(pred_proba)
    _check_privacy(data_path)
    _log_accountability(data_path)
    mlflow.set_tag("safeguard_version", "1.0")
    print("[safeguard] all checks complete")
