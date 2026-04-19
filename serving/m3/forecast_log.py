"""
forecast_log.py — Forecast event logger for M3 serving.

Each call to /forecast/features appends one record per category to a JSONL file.
The monitor daemon reads this log alongside ActualBudget monthly actuals to
compute per-category MAE for the deployed model version, driving the rollback
decision.

Log format (one JSON object per line):
    {
        "logged_at":      "2024-02-01T12:00:00Z",   # ISO-8601 UTC
        "forecast_month": "2024-02",                 # month being predicted
        "category":       "groceries",
        "forecast":       145.0,
        "model_version":  "3"
    }
"""
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

# Default path; overridden by M3_FORECAST_LOG_PATH env var.
_DEFAULT_LOG_PATH = "/data/m3_feedback/m3_forecasts.jsonl"

_lock = threading.Lock()


def _log_path() -> Path:
    return Path(os.environ.get("M3_FORECAST_LOG_PATH", _DEFAULT_LOG_PATH))


def log_forecasts(
    *,
    forecast_month: str,
    category_forecasts: list[tuple[str, float]],
    model_version: str | None,
) -> None:
    """Append one log entry per category to the forecast JSONL log.

    Args:
        forecast_month:     Target month in "YYYY-MM" format.
        category_forecasts: List of (category_name, forecast_amount) tuples.
        model_version:      Currently loaded model version string (e.g. "3").
    """
    if not category_forecasts:
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records = [
        json.dumps({
            "logged_at": now,
            "forecast_month": forecast_month,
            "category": cat,
            "forecast": round(amount, 4),
            "model_version": model_version or "unknown",
        })
        for cat, amount in category_forecasts
    ]
    payload = "\n".join(records) + "\n"

    log_file = _log_path()
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with _lock:
            with log_file.open("a", encoding="utf-8") as f:
                f.write(payload)
    except Exception as exc:
        # Logging must never crash the serving endpoint — swallow and warn.
        import logging
        logging.getLogger("forecast_log").warning(
            "Failed to write forecast log: %s", exc
        )


def read_forecasts_for_version(
    model_version: str,
    forecast_month: str | None = None,
) -> list[dict]:
    """Read all logged forecast records for a given model version.

    Optionally filtered to a specific forecast_month ("YYYY-MM").
    Returns a list of dicts matching the log format above.
    """
    log_file = _log_path()
    if not log_file.exists():
        return []

    results = []
    try:
        with log_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("model_version") != model_version:
                    continue
                if forecast_month and rec.get("forecast_month") != forecast_month:
                    continue
                results.append(rec)
    except Exception as exc:
        import logging
        logging.getLogger("forecast_log").warning(
            "Failed to read forecast log: %s", exc
        )
    return results


def compute_mae_vs_actuals(
    model_version: str,
    actuals: dict[str, dict[str, float]],
) -> dict[str, float]:
    """Compute per-category MAE for a model version against provided actuals.

    Args:
        model_version: Version string to evaluate (e.g. "3").
        actuals: Nested dict: {forecast_month: {category: actual_spend}}.
                 e.g. {"2024-02": {"groceries": 148.50, "restaurants": 32.10}}

    Returns:
        Dict of {category: mae} across all months where both forecast and
        actual are available. Categories with no matching actuals are omitted.
    """
    records = read_forecasts_for_version(model_version)
    if not records:
        return {}

    # Accumulate errors per category
    errors: dict[str, list[float]] = {}
    for rec in records:
        month = rec.get("forecast_month")
        cat = rec.get("category")
        forecast = rec.get("forecast")
        if not month or not cat or forecast is None:
            continue
        month_actuals = actuals.get(month, {})
        actual = month_actuals.get(cat)
        if actual is None:
            continue
        errors.setdefault(cat, []).append(abs(forecast - actual))

    return {cat: sum(errs) / len(errs) for cat, errs in errors.items() if errs}
