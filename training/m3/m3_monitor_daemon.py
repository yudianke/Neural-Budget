"""
M3 Monitor Daemon
=================
Runs continuously on the serving VM. Drives the monthly M3 retrain cycle and
provides automated rollback if the new model performs worse in production.

Lifecycle per calendar month:
  1. Day 2, 02:00 UTC — trigger retrain (calls run_m3_retrain.sh).
       • Records version_before from /health before triggering.
       • Verifies reload via /health until version_after appears (or timeout).
       • Writes event to state file.
  2. Day 16, 02:00 UTC — evaluate accuracy of newly deployed model.
       • Fetches /metrics/forecast-accuracy for the new version.
       • Compares overall MAE against the previous version's MAE (stored in state).
       • If new MAE > old MAE * ROLLBACK_MAE_DELTA:
           - Calls POST /admin/reload?version=<version_before>
           - Verifies rollback via /health
           - Logs rollback event to MLflow
       • Otherwise: records "ok" in state.

State is persisted to M3_STATE_PATH so the daemon survives restarts.

Environment variables (all have defaults):
    MLFLOW_TRACKING_URI     MLflow server URL
    M3_SERVING_URL          Base URL of the m3-serving FastAPI service
    M3_STATE_PATH           Path to persist daemon state JSON
    M3_RETRAIN_SCRIPT       Path to run_m3_retrain.sh
    M3_ACTUALS_URL          URL for fetching actuals (passed to /metrics/forecast-accuracy)
    CHECK_INTERVAL_SECONDS  Main loop poll interval (default: 3600 = 1 hour)
    M3_FORCE_RETRAIN        Set to "1" to bypass the day-of-month gate and retrain immediately
                            on the next poll. Useful for testing and manual triggers.
                            Resets automatically after one retrain fires.
    M3_FORCE_EVAL           Set to "1" to bypass the day-16 gate and run accuracy evaluation
                            immediately. Requires last_retrain_year_month == current month in state.
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import requests
from mlflow.tracking import MlflowClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MLFLOW_URI      = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.26.214:8000")
M3_SERVING_URL  = os.environ.get("M3_SERVING_URL", "http://m3-serving:8002")
STATE_PATH      = Path(os.environ.get("M3_STATE_PATH", "/data/m3_state/monitor_state.json"))
RETRAIN_SCRIPT  = os.environ.get("M3_RETRAIN_SCRIPT", "/app/training/m3/run_m3_retrain.sh")
ACTUALS_URL     = os.environ.get("M3_ACTUALS_URL", "")
CHECK_INTERVAL  = int(os.environ.get("CHECK_INTERVAL_SECONDS", "3600"))  # 1 hour
MODEL_NAME      = "m3-forecast"
EXPERIMENT_NAME = "m3-retrain-monitor"

# Rollback threshold: if new model's overall MAE is more than this factor worse,
# roll back to the previous version.
ROLLBACK_MAE_DELTA = float(os.environ.get("M3_ROLLBACK_MAE_DELTA", "1.20"))  # 20% worse

# How long to wait for /health to show the new version after a reload call.
RELOAD_VERIFY_TIMEOUT = int(os.environ.get("M3_RELOAD_VERIFY_TIMEOUT", "90"))

# Testing / manual trigger flags — bypass the day-of-month schedule gates.
FORCE_RETRAIN = os.environ.get("M3_FORCE_RETRAIN", "").strip().lower() in ("1", "true", "yes")
FORCE_EVAL    = os.environ.get("M3_FORCE_EVAL", "").strip().lower() in ("1", "true", "yes")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[M3-DAEMON %(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("m3_monitor_daemon")


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------
def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            pass
    return {
        # Retrain tracking
        "last_retrain_year_month": None,      # "YYYY-MM" of the last triggered retrain
        "version_before_retrain": None,       # model version before last retrain
        "version_after_retrain": None,        # model version confirmed after reload
        "version_before_mae": None,           # overall_mae of the version before retrain
        # Accuracy evaluation tracking
        "last_eval_year_month": None,         # "YYYY-MM" of the last accuracy evaluation
        "last_rollback_year_month": None,     # "YYYY-MM" of the last rollback (if any)
        # Failure backoff
        "consecutive_failures": 0,
        "last_failed_at": None,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# MLflow event logging
# ---------------------------------------------------------------------------
def _log_event_to_mlflow(event_type: str, params: dict) -> None:
    """Log a daemon lifecycle event as a short MLflow run."""
    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)
        with mlflow.start_run(run_name=f"m3-daemon-{event_type}"):
            mlflow.set_tag("event_type", event_type)
            for k, v in params.items():
                mlflow.set_tag(k, str(v))
    except Exception as exc:
        log.warning("Could not log event to MLflow: %s", exc)


# ---------------------------------------------------------------------------
# Serving health / version helpers
# ---------------------------------------------------------------------------
def _get_serving_version() -> str | None:
    """Return the currently loaded model version from /health, or None."""
    try:
        resp = requests.get(f"{M3_SERVING_URL}/health", timeout=5)
        data = resp.json()
        return data.get("model_version")
    except Exception as exc:
        log.warning("Could not reach m3-serving /health: %s", exc)
        return None


def _wait_for_version(expected_version: str, timeout: int = RELOAD_VERIFY_TIMEOUT) -> bool:
    """Poll /health until the loaded version matches expected_version or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = _get_serving_version()
        if current and str(current) == str(expected_version):
            return True
        time.sleep(5)
    return False


def _reload_serving(version: str | None = None) -> bool:
    """Call POST /admin/reload on m3-serving.

    If version is given, pins that version (rollback). Returns True if HTTP
    call succeeded (not necessarily that the reload completed).
    """
    params = {"version": version} if version else {}
    try:
        resp = requests.post(
            f"{M3_SERVING_URL}/admin/reload",
            params=params,
            timeout=10,
        )
        log.info("Reload response (pin=%s): %s", version, resp.json())
        return True
    except Exception as exc:
        log.error("Reload request failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Accuracy evaluation helper
# ---------------------------------------------------------------------------
def _get_forecast_accuracy(version: str) -> float | None:
    """Fetch overall MAE for a version from /metrics/forecast-accuracy.

    Returns None if the endpoint is unreachable or no MAE is available yet.
    """
    try:
        params: dict = {"version": version}
        if ACTUALS_URL:
            params["actuals_url"] = ACTUALS_URL
        resp = requests.get(
            f"{M3_SERVING_URL}/metrics/forecast-accuracy",
            params=params,
            timeout=15,
        )
        data = resp.json()
        mae = data.get("overall_mae")
        cat_count = data.get("categories_with_mae", 0)
        record_count = data.get("record_count", 0)
        log.info(
            "forecast-accuracy v%s: overall_mae=%s categories=%d records=%d",
            version, mae, cat_count, record_count,
        )
        return float(mae) if mae is not None else None
    except Exception as exc:
        log.warning("Could not fetch forecast-accuracy for v%s: %s", version, exc)
        return None


# ---------------------------------------------------------------------------
# Previous version MAE from MLflow
# ---------------------------------------------------------------------------
def _get_mlflow_mae_for_version(version: str) -> float | None:
    """Fetch the overall_mae metric logged in MLflow for a registered model version."""
    try:
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        ver_info = client.get_model_version(MODEL_NAME, version)
        run = client.get_run(ver_info.run_id)
        return run.data.metrics.get("overall_mae")
    except Exception as exc:
        log.warning("Could not fetch MLflow mae for v%s: %s", version, exc)
        return None


def _get_latest_registered_version() -> str | None:
    """Return the highest registered version number in MLflow, or None."""
    try:
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not versions:
            return None
        latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        return str(latest.version)
    except Exception as exc:
        log.warning("Could not query MLflow versions: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Phase 1 — Monthly retrain (runs on day 2 of each month)
# ---------------------------------------------------------------------------
def _run_retrain(state: dict) -> dict:
    """Trigger the retrain script and update state with before/after versions."""
    log.info("=== Phase 1: Monthly M3 Retrain ===")

    version_before = _get_serving_version()
    log.info("Version before retrain: %s", version_before)

    # Get eval MAE for version_before (from MLflow) so we have a baseline for rollback
    version_before_mae = None
    if version_before:
        version_before_mae = _get_mlflow_mae_for_version(version_before)
        log.info("Version-before MAE (from MLflow): %s", version_before_mae)

    # Run the retrain script
    env = {**os.environ, "M3_SERVING_URL": M3_SERVING_URL, "MLFLOW_TRACKING_URI": MLFLOW_URI}
    try:
        result = subprocess.run(
            [RETRAIN_SCRIPT],
            env=env,
            capture_output=False,  # let stdout/stderr flow to our log
            timeout=3600,          # 1-hour hard cap
        )
        exit_code = result.returncode
    except subprocess.TimeoutExpired:
        log.error("Retrain script timed out after 1 hour")
        exit_code = -1
    except Exception as exc:
        log.error("Retrain script error: %s", exc)
        exit_code = -2

    if exit_code != 0:
        log.error("Retrain script exited with code %d", exit_code)
        state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
        state["last_failed_at"] = datetime.now(timezone.utc).isoformat()
        _log_event_to_mlflow("retrain_failed", {
            "exit_code": exit_code,
            "version_before": version_before,
            "consecutive_failures": state["consecutive_failures"],
        })
        return state

    state["consecutive_failures"] = 0

    # Check what version is now registered and confirm the reload took effect
    version_after = _get_latest_registered_version()
    log.info("Latest MLflow version after retrain: %s", version_after)

    if version_after and version_after != str(version_before):
        # The retrain script handles reload; we just verify it took effect.
        log.info("Waiting for serving to load v%s ...", version_after)
        confirmed = _wait_for_version(version_after)
        if confirmed:
            log.info("Confirmed: serving is now running v%s", version_after)
        else:
            log.warning(
                "Serving did not switch to v%s within %ds — may need manual reload",
                version_after, RELOAD_VERIFY_TIMEOUT,
            )
    else:
        log.info("No new version registered (quality gates not passed) — no reload needed")
        version_after = None

    now_ym = datetime.now(timezone.utc).strftime("%Y-%m")
    state["last_retrain_year_month"] = now_ym
    state["version_before_retrain"] = str(version_before) if version_before else None
    state["version_after_retrain"] = str(version_after) if version_after else None
    state["version_before_mae"] = version_before_mae

    _log_event_to_mlflow("retrain_triggered", {
        "year_month": now_ym,
        "version_before": version_before,
        "version_after": version_after,
        "version_before_mae": version_before_mae,
        "exit_code": exit_code,
    })

    log.info("Phase 1 complete. before=%s after=%s", version_before, version_after)
    return state


# ---------------------------------------------------------------------------
# Phase 2 — Mid-month accuracy evaluation + rollback (runs on day 16)
# ---------------------------------------------------------------------------
def _run_accuracy_eval(state: dict) -> dict:
    """Evaluate the deployed model's accuracy against actuals; roll back if worse."""
    log.info("=== Phase 2: M3 Accuracy Evaluation + Rollback Check ===")

    version_after = state.get("version_after_retrain")
    version_before = state.get("version_before_retrain")
    version_before_mae = state.get("version_before_mae")

    if version_after is None:
        log.info("No new version was deployed last retrain cycle — skipping evaluation")
        now_ym = datetime.now(timezone.utc).strftime("%Y-%m")
        state["last_eval_year_month"] = now_ym
        return state

    # Confirm what version is actually serving right now
    current_version = _get_serving_version()
    log.info("Currently serving: v%s (expected: v%s)", current_version, version_after)

    # Fetch production MAE for the new version
    new_mae = _get_forecast_accuracy(str(version_after))
    log.info("Production MAE for v%s: %s", version_after, new_mae)

    now_ym = datetime.now(timezone.utc).strftime("%Y-%m")
    state["last_eval_year_month"] = now_ym

    if new_mae is None:
        # M3_ACTUALS_URL is not set or the endpoint returned no data yet.
        # Fall back to the training-time eval MAE logged to MLflow for this
        # version so rollback can still fire automatically without real actuals.
        mlflow_mae = _get_mlflow_mae_for_version(str(version_after))
        if mlflow_mae is not None:
            log.warning(
                "No production MAE for v%s (M3_ACTUALS_URL not set or no actuals yet). "
                "Falling back to MLflow training-eval MAE=%.4f for rollback decision. "
                "Set M3_ACTUALS_URL to an actuals endpoint for production-quality rollback.",
                version_after, mlflow_mae,
            )
            new_mae = mlflow_mae
        else:
            log.warning(
                "Could not compute production MAE for v%s — "
                "no actuals URL and no MLflow training MAE available. "
                "Skipping rollback check.",
                version_after,
            )
            _log_event_to_mlflow("eval_no_data", {
                "year_month": now_ym,
                "version_after": version_after,
                "version_before": version_before,
            })
            return state

    # Decide whether to roll back
    should_rollback = False
    rollback_reason = ""

    if version_before_mae is not None:
        threshold = version_before_mae * ROLLBACK_MAE_DELTA
        if new_mae > threshold:
            should_rollback = True
            rollback_reason = (
                f"new MAE {new_mae:.2f} > {threshold:.2f} "
                f"({ROLLBACK_MAE_DELTA:.0%} of previous {version_before_mae:.2f})"
            )
        else:
            log.info(
                "Model v%s is acceptable: new_mae=%.2f <= threshold=%.2f — no rollback",
                version_after, new_mae, threshold,
            )
    else:
        log.info(
            "No previous MAE baseline available — cannot compare. Keeping v%s.",
            version_after,
        )

    if should_rollback and version_before:
        log.warning("ROLLBACK TRIGGERED: %s", rollback_reason)
        log.info("Rolling back from v%s to v%s ...", version_after, version_before)

        reload_ok = _reload_serving(version=str(version_before))
        if reload_ok:
            confirmed = _wait_for_version(str(version_before))
            if confirmed:
                log.info("Rollback confirmed: serving is now running v%s", version_before)
                state["last_rollback_year_month"] = now_ym
                _log_event_to_mlflow("rollback", {
                    "year_month": now_ym,
                    "rolled_back_from": version_after,
                    "rolled_back_to": version_before,
                    "new_mae": new_mae,
                    "prev_mae": version_before_mae,
                    "rollback_reason": rollback_reason,
                })
            else:
                log.error(
                    "Rollback reload timed out — serving may be in an inconsistent state. "
                    "Manual intervention required."
                )
                _log_event_to_mlflow("rollback_failed", {
                    "year_month": now_ym,
                    "rolled_back_from": version_after,
                    "rolled_back_to": version_before,
                    "reason": "reload_timeout",
                })
        else:
            log.error("Rollback reload HTTP call failed.")
            _log_event_to_mlflow("rollback_failed", {
                "year_month": now_ym,
                "rolled_back_from": version_after,
                "rolled_back_to": version_before,
                "reason": "http_error",
            })
    elif should_rollback and not version_before:
        log.error(
            "Rollback needed but no version_before recorded in state — "
            "cannot roll back automatically. Manual intervention required."
        )
        _log_event_to_mlflow("rollback_impossible", {
            "year_month": now_ym,
            "version_after": version_after,
            "new_mae": new_mae,
            "rollback_reason": rollback_reason,
        })
    else:
        _log_event_to_mlflow("eval_ok", {
            "year_month": now_ym,
            "version_after": version_after,
            "new_mae": new_mae,
            "prev_mae": version_before_mae,
        })

    log.info("Phase 2 complete.")
    return state


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("M3 Monitor Daemon starting (check_interval=%ds)", CHECK_INTERVAL)
    log.info("M3 serving URL:  %s", M3_SERVING_URL)
    log.info("MLflow URI:      %s", MLFLOW_URI)
    log.info("State path:      %s", STATE_PATH)
    log.info("Retrain script:  %s", RETRAIN_SCRIPT)
    log.info("Actuals URL:     %s", ACTUALS_URL or "(not configured — rollback eval will be skipped)")
    log.info("Rollback delta:  %.0f%%", (ROLLBACK_MAE_DELTA - 1) * 100)
    if FORCE_RETRAIN:
        log.warning("M3_FORCE_RETRAIN=1 — day-of-month gate bypassed, retrain fires on next poll")
    if FORCE_EVAL:
        log.warning("M3_FORCE_EVAL=1 — day-16 gate bypassed, accuracy eval fires on next poll")

    mlflow.set_tracking_uri(MLFLOW_URI)

    while True:
        try:
            state = _load_state()
            now = datetime.now(timezone.utc)
            now_ym = now.strftime("%Y-%m")

            # ----------------------------------------------------------------
            # Phase 1 guard: trigger on day 2 of the month if not yet done.
            # M3_FORCE_RETRAIN=1 bypasses the day check for testing.
            # ----------------------------------------------------------------
            retrain_day_ok = FORCE_RETRAIN or now.day >= 2
            if (
                retrain_day_ok
                and state.get("last_retrain_year_month") != now_ym
                and state.get("consecutive_failures", 0) < 3
            ):
                if FORCE_RETRAIN:
                    log.info("FORCE_RETRAIN active — triggering retrain immediately")
                state = _run_retrain(state)
                _save_state(state)

            # ----------------------------------------------------------------
            # Phase 2 guard: evaluate on day 16 of the month if not yet done.
            # M3_FORCE_EVAL=1 bypasses the day check for testing.
            # ----------------------------------------------------------------
            eval_day_ok = FORCE_EVAL or now.day >= 16
            if (
                eval_day_ok
                and state.get("last_eval_year_month") != now_ym
                and state.get("last_retrain_year_month") == now_ym
            ):
                if FORCE_EVAL:
                    log.info("FORCE_EVAL active — triggering accuracy eval immediately")
                state = _run_accuracy_eval(state)
                _save_state(state)

            # Failure backoff: wait longer if recent failures piled up
            failures = state.get("consecutive_failures", 0)
            if failures >= 3:
                backoff = min(failures * 3600, 24 * 3600)
                log.error(
                    "%d consecutive retrain failures — backing off %dh before next attempt",
                    failures, backoff // 3600,
                )
                time.sleep(backoff)
                # Reset so it will retry next iteration
                state["consecutive_failures"] = 0
                state["last_retrain_year_month"] = None
                _save_state(state)
            else:
                time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            log.info("Daemon stopped by user")
            sys.exit(0)
        except Exception as exc:
            log.exception("Unexpected error in main loop: %s", exc)
            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
