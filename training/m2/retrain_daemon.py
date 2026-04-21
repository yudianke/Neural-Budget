"""
M2 Retrain Daemon
=================
Runs continuously on the training/serving VM. Monitors the feedback JSONL
file written by the m2-serving container and triggers retraining when:

  - >= 30 new dismissals (feedback_type == "dismiss_false_positive") since last retrain
  - OR every Sunday at midnight (weekly schedule)
  - OR dismiss_rate > 50% (urgent: too many false positives)

Key difference from M1 daemon:
  - IsolationForest is UNSUPERVISED — feedback does NOT become labels.
  - High dismiss rate -> retrain with LOWER contamination to reduce FP.
  - The daemon adjusts M2_CONTAMINATION env var before retraining.
  - Rollback trigger: dismiss_rate increased by > ROLLBACK_RATE_DELTA.

After a successful retrain + MLflow model registration:
  - Exports new model to ONNX (via export_to_onnx.py)
  - Calls POST /admin/reload on the m2-serving container (hot-reload)
  - Waits ROLLBACK_WINDOW_HOURS, then checks if dismiss rate got worse
  - If dismiss rate increased by > ROLLBACK_RATE_DELTA: rolls back

State is persisted to DAEMON_STATE_PATH so the daemon survives restarts.

Environment variables (all have defaults):
  MLFLOW_TRACKING_URI       MLflow server URL
  M2_SERVING_URL            Base URL of the m2-serving FastAPI container
  M2_FEEDBACK_LOG_PATH      Path to the feedback JSONL file (shared volume)
  M2_DAEMON_STATE_PATH      Path to persist daemon state JSON
  M2_CONFIG                 Path to config_m2.yaml
  M2_BOOTSTRAP_DATA_PATH    Path to bootstrap training data
  CHECK_INTERVAL_SECONDS    How often to poll (default 300 = 5 min)
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from mlflow.tracking import MlflowClient

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
MLFLOW_URI        = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.26.214:8000")
M2_SERVING_URL    = os.environ.get("M2_SERVING_URL", "http://m2-serving:8003")
FEEDBACK_PATH     = Path(os.environ.get("M2_FEEDBACK_LOG_PATH", "/data/feedback/m2_feedback.jsonl"))
STATE_PATH        = Path(os.environ.get("M2_DAEMON_STATE_PATH", "/data/feedback/m2_daemon_state.json"))
CONFIG_PATH       = os.environ.get("M2_CONFIG", "/app/training/m2/config_m2.yaml")
BOOTSTRAP_PATH    = os.environ.get("M2_BOOTSTRAP_DATA_PATH", "")  # empty = use config_m2.yaml train_path (swift://)
CHECK_INTERVAL    = int(os.environ.get("CHECK_INTERVAL_SECONDS", "300"))
MODEL_NAME        = "m2-anomaly"

DISMISS_THRESHOLD         = 30      # new dismissals since last retrain -> trigger
DISMISS_RATE_URGENT       = 0.50    # dismiss rate > 50% -> urgent retrain
ROLLBACK_RATE_DELTA       = 0.20    # 20% worse dismiss rate -> rollback
ROLLBACK_WINDOW_HOURS     = 24      # hours to wait before evaluating rollback
FAILURE_BACKOFF_MINUTES   = 60      # wait after failed retrain
RELOAD_MAX_RETRIES        = 3

# Contamination adjustment: if dismiss_rate is high, lower contamination
CONTAMINATION_DEFAULT     = 0.05
CONTAMINATION_MIN         = 0.01
CONTAMINATION_STEP        = 0.01    # reduce by this much per high-dismiss retrain

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[M2-DAEMON %(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("m2_retrain_daemon")


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
        "last_retrain_at": None,
        "last_retrain_version": None,
        "last_dismiss_count": 0,
        "prev_dismiss_rate": None,
        "rollback_check_due": None,
        "consecutive_failures": 0,
        "last_failed_at": None,
        "version_before_last_retrain": None,
        "pending_reload_version": None,
        "pending_reload_retries": 0,
        "current_contamination": CONTAMINATION_DEFAULT,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Feedback counting
# ---------------------------------------------------------------------------
def _count_dismissals_total() -> int:
    if not FEEDBACK_PATH.exists():
        return 0
    total = 0
    with FEEDBACK_PATH.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if row.get("feedback_type") == "dismiss_false_positive":
                    total += 1
            except json.JSONDecodeError:
                continue
    return total


def _get_dismiss_rate_for_version(version: str) -> float | None:
    try:
        resp = requests.get(
            f"{M2_SERVING_URL}/metrics/feedback/since/{version}",
            timeout=10,
        )
        if resp.ok:
            data = resp.json()
            return data.get("dismiss_rate")
    except Exception as e:
        log.warning(f"Could not fetch dismiss rate for version {version}: {e}")
    return None


def _get_current_dismiss_rate() -> float | None:
    try:
        resp = requests.get(f"{M2_SERVING_URL}/metrics/feedback", timeout=10)
        if resp.ok:
            data = resp.json()
            return data.get("dismiss_rate")
    except Exception as e:
        log.warning(f"Could not fetch current dismiss rate: {e}")
    return None


# ---------------------------------------------------------------------------
# MLflow helpers
# ---------------------------------------------------------------------------
def _get_latest_version() -> int | None:
    try:
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not versions:
            return None
        versions = sorted(versions, key=lambda v: int(v.version), reverse=True)
        return int(versions[0].version)
    except Exception as e:
        log.warning(f"MLflow version query failed: {e}")
        return None


def _log_event_to_mlflow(event_type: str, details: dict) -> None:
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("m2-retrain-daemon")
        with mlflow.start_run(run_name=f"daemon_{event_type}"):
            mlflow.set_tag("event_type", event_type)
            mlflow.set_tag("daemon_version", "1.0")
            for k, v in details.items():
                mlflow.set_tag(k, str(v))
            log.info(f"Logged {event_type} event to MLflow")
    except Exception as e:
        log.warning(f"MLflow event logging failed (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Retrain
# ---------------------------------------------------------------------------
def _compute_contamination(state: dict) -> float:
    """Adjust contamination based on dismiss rate feedback.

    If dismiss_rate is high (> 30%), lower contamination to reduce FP.
    This is the key adaptation for unsupervised IsolationForest:
    feedback drives threshold tuning, not label training.
    """
    current = state.get("current_contamination", CONTAMINATION_DEFAULT)
    dismiss_rate = _get_current_dismiss_rate()

    if dismiss_rate is not None and dismiss_rate > 0.30:
        new_val = max(current - CONTAMINATION_STEP, CONTAMINATION_MIN)
        if new_val != current:
            log.info(
                f"Adjusting contamination {current:.3f} -> {new_val:.3f} "
                f"(dismiss_rate={dismiss_rate:.2%})"
            )
            return new_val
    return current


def _run_retrain(contamination: float) -> bool:
    script = Path(__file__).parent / "run_m2_retrain.sh"
    if not script.exists():
        log.error(f"Retrain script not found: {script}")
        return False

    env = os.environ.copy()
    env["M2_FEEDBACK_INPUT"] = str(FEEDBACK_PATH)
    env["M2_FEEDBACK_DATASET"] = str(FEEDBACK_PATH.parent / "m2_feedback_dataset.csv")
    env["M2_CONFIG"] = CONFIG_PATH
    # Only set M2_TRAIN_PATH if explicitly provided — otherwise train_m2.py
    # reads train_path from config_m2.yaml (swift:// on Chameleon S3)
    if BOOTSTRAP_PATH:
        env["M2_TRAIN_PATH"] = BOOTSTRAP_PATH
    env["MLFLOW_TRACKING_URI"] = MLFLOW_URI
    # Override contamination for this retrain
    env["M2_CONTAMINATION"] = str(contamination)

    log.info(f"Starting retraining with contamination={contamination:.3f}...")
    try:
        result = subprocess.run(
            ["bash", str(script)],
            env=env,
            capture_output=False,
            timeout=3600,
        )
        if result.returncode == 0:
            log.info("Retraining completed successfully")
            return True
        else:
            log.error(f"Retraining exited with code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        log.error("Retraining timed out after 1 hour")
        return False
    except Exception as e:
        log.error(f"Retraining failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Hot-reload + verification
# ---------------------------------------------------------------------------
def _trigger_reload() -> bool:
    try:
        resp = requests.post(f"{M2_SERVING_URL}/admin/reload", timeout=10)
        if resp.ok:
            log.info(f"Reload triggered: {resp.json()}")
            return True
        log.warning(f"Reload returned {resp.status_code}: {resp.text}")
    except Exception as e:
        log.warning(f"Reload request failed: {e}")
    return False


def _verify_deployed_version(expected_version: str, wait_seconds: int = 15) -> bool:
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        try:
            resp = requests.get(f"{M2_SERVING_URL}/health", timeout=5)
            if resp.ok:
                data = resp.json()
                actual = str(data.get("model_version", ""))
                if expected_version in actual:
                    log.info(f"Confirmed deployed version: {actual}")
                    return True
        except Exception as e:
            log.warning(f"Health check failed: {e}")
        time.sleep(3)
    log.warning(f"Version verification timed out after {wait_seconds}s")
    return False


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
def _rollback_to_version(version: int) -> None:
    log.warning(f"Rolling back to version {version}...")
    try:
        resp = requests.post(
            f"{M2_SERVING_URL}/admin/reload",
            params={"version": str(version)},
            timeout=15,
        )
        if resp.ok:
            log.info(f"Rollback reload triggered: {resp.json()}")
            time.sleep(10)
            health = requests.get(f"{M2_SERVING_URL}/health", timeout=5).json()
            loaded = health.get("model_version")
            log.info(f"Rollback result: serving version {loaded}")

            _log_event_to_mlflow("rollback", {
                "rollback_to_version": str(version),
                "loaded_version": str(loaded),
                "reason": "dismiss_rate_degraded",
            })
        else:
            log.error(f"Rollback returned {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Rollback failed: {e}")


# ---------------------------------------------------------------------------
# Weekly trigger
# ---------------------------------------------------------------------------
def _should_weekly_trigger(state: dict) -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() != 6:  # Sunday
        return False
    if state["last_retrain_at"] is None:
        return True
    last = datetime.fromisoformat(state["last_retrain_at"])
    return (now - last).days >= 6


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run():
    log.info(f"M2 Retrain daemon starting | model={MODEL_NAME} | mlflow={MLFLOW_URI}")
    log.info(f"Feedback path: {FEEDBACK_PATH}")
    log.info(f"Check interval: {CHECK_INTERVAL}s | dismiss threshold: {DISMISS_THRESHOLD}")

    while True:
        try:
            state = _load_state()
            now_iso = datetime.now(timezone.utc).isoformat()

            # ----------------------------------------------------------------
            # 1. Check rollback window
            # ----------------------------------------------------------------
            if state.get("rollback_check_due"):
                due = datetime.fromisoformat(state["rollback_check_due"])
                if datetime.now(timezone.utc) >= due:
                    log.info("Rollback evaluation window reached...")
                    last_version = state.get("last_retrain_version")
                    prev_rate = state.get("prev_dismiss_rate")

                    if last_version and prev_rate is not None:
                        current_rate = _get_dismiss_rate_for_version(str(last_version))
                        if current_rate is not None:
                            log.info(f"Dismiss rate: prev={prev_rate:.3f} new={current_rate:.3f}")
                            if current_rate > prev_rate * (1 + ROLLBACK_RATE_DELTA):
                                log.warning(
                                    f"Dismiss rate degraded {prev_rate:.1%} -> {current_rate:.1%}. "
                                    f"Rolling back."
                                )
                                rollback_target = state.get("version_before_last_retrain")
                                if rollback_target is None:
                                    rollback_target = str(int(last_version) - 1)
                                if int(rollback_target) >= 1:
                                    _rollback_to_version(int(rollback_target))
                            else:
                                log.info("Dismiss rate acceptable — keeping new model.")
                                _log_event_to_mlflow("rollback_check_passed", {
                                    "version": last_version,
                                    "prev_rate": prev_rate,
                                    "current_rate": current_rate,
                                })

                    state["rollback_check_due"] = None
                    _save_state(state)

            # ----------------------------------------------------------------
            # 1b. Retry pending reload
            # ----------------------------------------------------------------
            if state.get("pending_reload_version"):
                pending_ver = state["pending_reload_version"]
                retries = state.get("pending_reload_retries", 0)
                log.info(f"Retrying pending reload v{pending_ver} (attempt {retries + 1})")

                retry_ok = _trigger_reload()
                time.sleep(8)
                retry_verified = False
                if retry_ok:
                    retry_verified = _verify_deployed_version(pending_ver, wait_seconds=30)

                if retry_ok and retry_verified:
                    log.info(f"Pending reload v{pending_ver} confirmed")
                    state["last_dismiss_count"] = _count_dismissals_total()
                    state["last_retrain_version"] = pending_ver
                    state["pending_reload_version"] = None
                    state["pending_reload_retries"] = 0
                    _save_state(state)
                else:
                    retries += 1
                    state["pending_reload_retries"] = retries
                    if retries >= RELOAD_MAX_RETRIES:
                        log.error(f"Pending reload failed after {RELOAD_MAX_RETRIES} attempts")
                        state["last_dismiss_count"] = _count_dismissals_total()
                        state["pending_reload_version"] = None
                        state["pending_reload_retries"] = 0
                    _save_state(state)
                time.sleep(CHECK_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # 2. Check retrain triggers
            # ----------------------------------------------------------------
            current_dismissals = _count_dismissals_total()
            last_dismissals = state.get("last_dismiss_count", 0)
            new_dismissals = current_dismissals - last_dismissals

            weekly = _should_weekly_trigger(state)
            dismiss_trigger = new_dismissals >= DISMISS_THRESHOLD

            # Urgent trigger: high dismiss rate
            current_rate = _get_current_dismiss_rate()
            urgent = current_rate is not None and current_rate > DISMISS_RATE_URGENT

            if not dismiss_trigger and not weekly and not urgent:
                log.info(
                    f"No trigger: new_dismissals={new_dismissals}/{DISMISS_THRESHOLD}, "
                    f"weekly={weekly}, urgent={urgent}"
                )
                time.sleep(CHECK_INTERVAL)
                continue

            trigger_reason = (
                f"urgent_dismiss_rate={current_rate:.2%}" if urgent
                else "weekly_schedule" if weekly
                else f"dismissals={new_dismissals}"
            )
            log.info(f"RETRAIN TRIGGERED: {trigger_reason}")

            # Backoff check
            consecutive_failures = state.get("consecutive_failures", 0)
            last_failed_at = state.get("last_failed_at")
            if consecutive_failures > 0 and last_failed_at:
                backoff_until = datetime.fromisoformat(last_failed_at) + timedelta(
                    minutes=FAILURE_BACKOFF_MINUTES * consecutive_failures
                )
                if datetime.now(timezone.utc) < backoff_until:
                    log.warning(f"Skipping — in backoff until {backoff_until.isoformat()}")
                    time.sleep(CHECK_INTERVAL)
                    continue

            version_before = _get_latest_version()
            prev_rate = _get_dismiss_rate_for_version(
                str(version_before)
            ) if version_before else None

            # Compute adjusted contamination
            contamination = _compute_contamination(state)

            # ----------------------------------------------------------------
            # 3. Run retraining
            # ----------------------------------------------------------------
            retrain_ok = _run_retrain(contamination)

            if not retrain_ok:
                consecutive_failures = state.get("consecutive_failures", 0) + 1
                state["consecutive_failures"] = consecutive_failures
                state["last_failed_at"] = now_iso
                state["last_retrain_at"] = now_iso
                _save_state(state)
                log.error(f"Retrain failed (failures={consecutive_failures})")
                _log_event_to_mlflow("retrain_failed", {
                    "trigger": trigger_reason,
                    "version_before": str(version_before),
                    "consecutive_failures": str(consecutive_failures),
                })
                time.sleep(CHECK_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # 4. Check new version
            # ----------------------------------------------------------------
            version_after = _get_latest_version()
            if version_after is None or version_after == version_before:
                log.info("No new version registered — model did not improve.")
                _log_event_to_mlflow("retrain_no_improvement", {
                    "trigger": trigger_reason,
                    "version": str(version_before),
                })
                state["last_dismiss_count"] = current_dismissals
                state["last_retrain_at"] = now_iso
                state["consecutive_failures"] = 0
                state["last_failed_at"] = None
                _save_state(state)
                time.sleep(CHECK_INTERVAL)
                continue

            log.info(f"New version: v{version_before} -> v{version_after}")

            # ----------------------------------------------------------------
            # 5. Hot-reload
            # ----------------------------------------------------------------
            reload_ok = _trigger_reload()
            verified = False
            if reload_ok:
                verified = _verify_deployed_version(str(version_after), wait_seconds=30)

            if not reload_ok or not verified:
                log.error(f"Reload failed for v{version_after}")
                state["last_retrain_at"] = now_iso
                state["consecutive_failures"] = 0
                state["last_failed_at"] = None
                state["pending_reload_version"] = str(version_after)
                state["pending_reload_retries"] = 0
                state["version_before_last_retrain"] = str(version_before) if version_before else None
                _save_state(state)
                time.sleep(CHECK_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # 6. Update state + schedule rollback check
            # ----------------------------------------------------------------
            rollback_due = (
                datetime.now(timezone.utc) + timedelta(hours=ROLLBACK_WINDOW_HOURS)
            ).isoformat()

            state["last_retrain_at"] = now_iso
            state["last_retrain_version"] = str(version_after)
            state["last_dismiss_count"] = current_dismissals
            state["prev_dismiss_rate"] = prev_rate
            state["rollback_check_due"] = rollback_due
            state["consecutive_failures"] = 0
            state["last_failed_at"] = None
            state["version_before_last_retrain"] = str(version_before) if version_before else None
            state["current_contamination"] = contamination
            _save_state(state)

            _log_event_to_mlflow("retrain_success", {
                "trigger": trigger_reason,
                "version_before": str(version_before),
                "version_after": str(version_after),
                "new_dismissals": str(new_dismissals),
                "contamination": str(contamination),
                "rollback_check_due": rollback_due,
            })

            log.info(
                f"Done. v{version_after} loaded. "
                f"Rollback check at {rollback_due}"
            )

        except Exception as e:
            log.exception(f"Unexpected error in daemon loop: {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
