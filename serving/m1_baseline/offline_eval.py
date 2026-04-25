"""Offline evaluation gate for M1 serving.

Runs held-out accuracy / per-category F1 checks at model load time
*before* the model becomes active. Prevents bad models from serving.

Two eval sources:
  1. MLflow eval_data artifact  — uploaded by train_m1_ray.py (synthetic eval split)
  2. Built-in sanity cases      — hardcoded merchant→category pairs for quick checks

Fail-open for infra issues (MLflow down, artifact missing).
Fail-closed for quality issues (model accuracy below threshold).
"""

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import xgboost as xgb
from scipy.sparse import csr_matrix, hstack
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
EVAL_GATE_ENABLED = os.environ.get("M1_EVAL_GATE_ENABLED", "true").lower() == "true"
SYNTHETIC_GATE_FLOOR = float(os.environ.get("M1_SYNTHETIC_GATE_FLOOR", "0.85"))
SANITY_GATE_FLOOR = float(os.environ.get("M1_SANITY_GATE_FLOOR", "0.70"))
FAIRNESS_F1_FLOOR = 0.40  # categories below this are flagged

# Text/numeric columns must match real_model.py exactly
TEXT_COL = "merchant"
NUMERIC_COLS = ["log_amount", "day_of_week", "day_of_month"]

# Built-in sanity test cases — known merchant→category pairs from MoneyData
# and the synthetic data generator. These should always work if the model
# is functioning correctly.
SANITY_CASES = [
    # (merchant_raw, expected_category, description)
    ("SUBWAY 26377 CORNE", "restaurants", "Fast food chain"),
    ("SUBWAY 32610 MIDLA", "restaurants", "Subway variant"),
    ("TESCO FABIAN W", "groceries", "UK supermarket"),
    ("LIDL GB NOTTINGHAM", "groceries", "Discount supermarket"),
    ("MARKS&SPENCER PLC", "groceries", "M&S"),
    ("AMAZON UK MARKETPL", "shopping", "Amazon marketplace"),
    ("AMZNMKTPLACE", "shopping", "Amazon abbreviated"),
    ("VIRGIN MEDIA PYMTS", "utilities", "Broadband provider"),
    ("GOOD ENERGY LTD", "utilities", "Energy provider"),
    ("UBER   *TRIP", "transport", "Ride-share"),
    ("ARRIVA TRAINS WALE", "transport", "Train operator"),
    ("SPOTIFY", "entertainment", "Streaming service"),
    ("NETFLIX", "entertainment", "Streaming service"),
    ("SWANSEA UNIVERSITY", "education", "University"),
    ("LV LIFE", "insurance", "Life insurance"),
    ("TRADING212UK", "misc", "Trading platform"),
    ("NON-GBP TRANS FEE", "misc", "Bank fee"),
]


@dataclass
class EvalResults:
    """Results from an offline evaluation run."""
    source: str                           # "synthetic", "sanity", "moneydata"
    accuracy: float = 0.0
    macro_f1: float = 0.0
    weighted_f1: float = 0.0
    per_category_f1: dict = field(default_factory=dict)
    underperforming: list = field(default_factory=list)
    confusion_top5: list = field(default_factory=list)
    eval_size: int = 0
    gate_passed: bool = True
    gate_floor: float = 0.0
    gate_reason: str = ""
    timestamp: str = ""
    model_version: str = ""
    duration_ms: float = 0.0

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "accuracy": round(self.accuracy, 4),
            "macro_f1": round(self.macro_f1, 4),
            "weighted_f1": round(self.weighted_f1, 4),
            "per_category_f1": {k: round(v, 4) for k, v in self.per_category_f1.items()},
            "underperforming_categories": self.underperforming,
            "confusion_top5": self.confusion_top5,
            "eval_size": self.eval_size,
            "gate_passed": self.gate_passed,
            "gate_floor": self.gate_floor,
            "gate_reason": self.gate_reason,
            "timestamp": self.timestamp,
            "model_version": self.model_version,
            "duration_ms": round(self.duration_ms, 1),
        }


def _normalize_merchant(name: str) -> str:
    """Mirror real_model.normalize_merchant exactly."""
    if not isinstance(name, str):
        return ""
    value = name.upper().strip()
    value = re.sub(r"\b\d{4,}\b", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _predict_batch(
    merchants: list[str],
    amounts: list[float],
    dates: list[str],
    booster: xgb.Booster,
    tfidf,
    label_encoder,
    metadata: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Run batch prediction. Returns (predicted_indices, probabilities)."""
    rows = []
    for merchant, amount, date_str in zip(merchants, amounts, dates):
        clean = _normalize_merchant(merchant)
        log_amount = float(np.log1p(abs(amount)))
        try:
            d = pd.Timestamp(date_str)
            dow = d.dayofweek
            dom = d.day
        except Exception:
            dow, dom = 0, 1
        rows.append({
            TEXT_COL: clean,
            "log_amount": log_amount,
            "day_of_week": dow,
            "day_of_month": dom,
        })

    df = pd.DataFrame(rows)
    text_vec = tfidf.transform(df[TEXT_COL].astype(str))
    num_vec = csr_matrix(df[NUMERIC_COLS].values.astype(float))
    features = hstack([text_vec, num_vec], format="csr").astype(np.float32)
    feature_names = metadata.get("feature_columns")
    dmatrix = xgb.DMatrix(features, feature_names=feature_names)
    pred_proba = booster.predict(dmatrix)

    if pred_proba.ndim == 2:
        pred_indices = np.argmax(pred_proba, axis=1)
    else:
        pred_indices = pred_proba.astype(int)

    return pred_indices, pred_proba


def _compute_eval_results(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    label_encoder,
    source: str,
    gate_floor: float,
    model_version: str,
    start_time: float,
) -> EvalResults:
    """Compute accuracy, F1, confusion matrix from true/pred arrays."""
    accuracy = float(accuracy_score(y_true, y_pred))
    macro_f1 = float(f1_score(y_true, y_pred, average="macro", zero_division=0))
    weighted_f1 = float(f1_score(y_true, y_pred, average="weighted", zero_division=0))

    # Per-category F1
    labels = np.unique(np.concatenate([y_true, y_pred]))
    per_cat_f1_values = f1_score(y_true, y_pred, labels=labels, average=None, zero_division=0)
    per_category_f1 = {}
    underperforming = []
    for idx, f1_val in zip(labels, per_cat_f1_values):
        cat_name = str(label_encoder.inverse_transform([int(idx)])[0])
        per_category_f1[cat_name] = float(f1_val)
        if f1_val < FAIRNESS_F1_FLOOR:
            underperforming.append(f"{cat_name}={f1_val:.3f}")

    # Confusion matrix — top 5 most confused pairs
    confusion_top5 = []
    if len(labels) > 1:
        cm = confusion_matrix(y_true, y_pred, labels=labels)
        for i, row_label in enumerate(labels):
            for j, col_label in enumerate(labels):
                if i != j and cm[i][j] > 0:
                    confusion_top5.append({
                        "true": str(label_encoder.inverse_transform([int(row_label)])[0]),
                        "predicted": str(label_encoder.inverse_transform([int(col_label)])[0]),
                        "count": int(cm[i][j]),
                    })
        confusion_top5.sort(key=lambda x: x["count"], reverse=True)
        confusion_top5 = confusion_top5[:5]

    gate_passed = macro_f1 >= gate_floor
    gate_reason = (
        f"macro_f1={macro_f1:.4f} >= {gate_floor}" if gate_passed
        else f"macro_f1={macro_f1:.4f} < {gate_floor} FAILED"
    )

    return EvalResults(
        source=source,
        accuracy=accuracy,
        macro_f1=macro_f1,
        weighted_f1=weighted_f1,
        per_category_f1=per_category_f1,
        underperforming=underperforming,
        confusion_top5=confusion_top5,
        eval_size=len(y_true),
        gate_passed=gate_passed,
        gate_floor=gate_floor,
        gate_reason=gate_reason,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        model_version=model_version,
        duration_ms=(time.time() - start_time) * 1000,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_sanity_eval(
    booster: xgb.Booster,
    tfidf,
    label_encoder,
    metadata: dict,
    model_version: str = "",
) -> EvalResults:
    """Run built-in sanity cases against the loaded model.

    These are hardcoded merchant→category pairs that should always be correct
    if the model is functioning. Uses a lower gate floor since these are
    hand-picked and may include edge cases.
    """
    start = time.time()
    merchants = [c[0] for c in SANITY_CASES]
    amounts = [-10.0] * len(SANITY_CASES)  # arbitrary negative (debit)
    dates = ["2024-06-15"] * len(SANITY_CASES)

    pred_indices, _ = _predict_batch(
        merchants, amounts, dates, booster, tfidf, label_encoder, metadata
    )

    # Map expected categories to label encoder indices
    known_classes = set(label_encoder.classes_)
    y_true_list = []
    y_pred_list = []
    skipped = 0
    for i, (merchant, expected_cat, desc) in enumerate(SANITY_CASES):
        if expected_cat not in known_classes:
            skipped += 1
            continue
        true_idx = int(label_encoder.transform([expected_cat])[0])
        y_true_list.append(true_idx)
        y_pred_list.append(int(pred_indices[i]))

    if not y_true_list:
        return EvalResults(
            source="sanity",
            gate_passed=True,
            gate_reason="no sanity cases matched model classes — skipped",
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            model_version=model_version,
            duration_ms=(time.time() - start) * 1000,
        )

    y_true = np.array(y_true_list)
    y_pred = np.array(y_pred_list)

    result = _compute_eval_results(
        y_true, y_pred, label_encoder, "sanity",
        SANITY_GATE_FLOOR, model_version, start,
    )
    if skipped:
        result.gate_reason += f" (skipped {skipped} cases with unknown categories)"
    return result


def run_synthetic_eval(
    booster: xgb.Booster,
    tfidf,
    label_encoder,
    metadata: dict,
    eval_df: pd.DataFrame,
    model_version: str = "",
) -> EvalResults:
    """Run evaluation on synthetic held-out data downloaded from MLflow.

    Args:
        eval_df: DataFrame with columns [date, merchant, amount, category]
    """
    start = time.time()
    label_col = "category"
    if label_col not in eval_df.columns:
        # Try alternative column names
        for alt in ["project_category", "label"]:
            if alt in eval_df.columns:
                label_col = alt
                break

    known_classes = set(label_encoder.classes_)
    eval_df = eval_df[eval_df[label_col].isin(known_classes)].copy()

    if len(eval_df) == 0:
        return EvalResults(
            source="synthetic",
            gate_passed=True,
            gate_reason="no eval rows matched model classes — skipped",
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            model_version=model_version,
            duration_ms=(time.time() - start) * 1000,
        )

    # Cap eval to 10k rows for speed at load time
    max_eval = int(os.environ.get("M1_EVAL_MAX_ROWS", "10000"))
    if len(eval_df) > max_eval:
        eval_df = eval_df.sample(n=max_eval, random_state=42)

    merchants = eval_df[TEXT_COL].tolist()
    amounts = eval_df["amount"].tolist()
    dates = eval_df["date"].astype(str).tolist()

    pred_indices, _ = _predict_batch(
        merchants, amounts, dates, booster, tfidf, label_encoder, metadata
    )

    y_true = label_encoder.transform(eval_df[label_col])
    y_pred = pred_indices.astype(int)

    return _compute_eval_results(
        y_true, y_pred, label_encoder, "synthetic",
        SYNTHETIC_GATE_FLOOR, model_version, start,
    )


def download_eval_data(run_id: str, tracking_uri: str) -> Optional[pd.DataFrame]:
    """Download eval_data/eval_holdout.csv from MLflow for a given run.

    Returns None if artifact doesn't exist (older model versions)
    or if MLflow is unreachable. Never raises — fail-open.
    """
    try:
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        artifact_path = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path="eval_data"
        )
        csv_path = Path(artifact_path) / "eval_holdout.csv"
        if not csv_path.exists():
            print("[M1-EVAL] eval_data artifact directory exists but eval_holdout.csv not found")
            return None
        df = pd.read_csv(csv_path)
        print(f"[M1-EVAL] downloaded eval holdout: {len(df):,} rows from run {run_id[:8]}")
        return df
    except Exception as e:
        print(f"[M1-EVAL] eval_data download skipped ({e}) — will use sanity eval only")
        return None


def run_full_eval(
    booster: xgb.Booster,
    tfidf,
    label_encoder,
    metadata: dict,
    model_version: str = "",
    run_id: str = "",
    tracking_uri: str = "",
) -> dict:
    """Run all available evaluations. Returns combined results dict.

    Called by real_model.load() at startup and reload().
    Gate logic:
      - Sanity eval must pass (gate_floor=0.70)
      - Synthetic eval must pass if data is available (gate_floor=0.85)
      - If both are available, both must pass
      - If only sanity is available, sanity alone decides
    """
    if not EVAL_GATE_ENABLED:
        return {
            "enabled": False,
            "gate_passed": True,
            "reason": "eval gate disabled via M1_EVAL_GATE_ENABLED=false",
        }

    results = {"enabled": True, "evaluations": {}}

    # 1. Built-in sanity eval (always available)
    sanity = run_sanity_eval(booster, tfidf, label_encoder, metadata, model_version)
    results["evaluations"]["sanity"] = sanity.to_dict()
    print(
        f"[M1-EVAL] sanity: accuracy={sanity.accuracy:.4f} macro_f1={sanity.macro_f1:.4f} "
        f"gate={'PASS' if sanity.gate_passed else 'FAIL'} ({sanity.eval_size} cases, {sanity.duration_ms:.0f}ms)"
    )

    # 2. Synthetic eval (from MLflow, if available)
    synthetic = None
    if run_id and tracking_uri:
        eval_df = download_eval_data(run_id, tracking_uri)
        if eval_df is not None:
            synthetic = run_synthetic_eval(
                booster, tfidf, label_encoder, metadata, eval_df, model_version
            )
            results["evaluations"]["synthetic"] = synthetic.to_dict()
            print(
                f"[M1-EVAL] synthetic: accuracy={synthetic.accuracy:.4f} macro_f1={synthetic.macro_f1:.4f} "
                f"gate={'PASS' if synthetic.gate_passed else 'FAIL'} ({synthetic.eval_size} rows, {synthetic.duration_ms:.0f}ms)"
            )

    # 3. Compute overall gate
    all_passed = sanity.gate_passed
    reasons = [f"sanity: {sanity.gate_reason}"]

    if synthetic:
        all_passed = all_passed and synthetic.gate_passed
        reasons.append(f"synthetic: {synthetic.gate_reason}")

    results["gate_passed"] = all_passed
    results["reason"] = " | ".join(reasons)
    results["model_version"] = model_version

    status = "PASS" if all_passed else "FAIL"
    print(f"[M1-EVAL] overall gate: {status} — {results['reason']}")

    return results
