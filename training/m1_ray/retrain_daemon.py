"""
M1 Retrain Daemon
=================
Runs continuously on the training/serving VM. Monitors the feedback JSONL
file written by the m1-serving container and triggers retraining when:

  - ≥ 20 new corrections (feedback_type == "overridden") since the last retrain
  - OR every Sunday at midnight (weekly schedule)

After a successful retrain + MLflow model registration:
  - Calls POST /admin/reload on the m1-serving container (hot-reload)
  - Waits ROLLBACK_WINDOW_HOURS, then checks if the correction rate got worse
  - If correction rate increased by > ROLLBACK_RATE_DELTA: rolls back to the
    previous model version by restarting m1-serving with M1_MODEL_VERSION pinned

State is persisted to DAEMON_STATE_PATH so the daemon survives restarts.

Environment variables (all have defaults):
  MLFLOW_TRACKING_URI       MLflow server URL
  M1_SERVING_URL            Base URL of the m1-serving FastAPI container
  M1_FEEDBACK_LOG_PATH      Path to the feedback JSONL file (shared volume)
  M1_DAEMON_STATE_PATH      Path to persist daemon state JSON
  M1_RAY_CONFIG             Path to config_m1_ray.yaml
  M1_BOOTSTRAP_DATA_PATH    Path to categorization_train CSV (swift:// or local; overrides config data_path)
  M1_SERVING_CONTAINER      Docker container name for m1-serving
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
MLFLOW_URI        = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.25.192:8000")
M1_SERVING_URL    = os.environ.get("M1_SERVING_URL", "http://m1-serving:8001")
FEEDBACK_PATH     = Path(os.environ.get("M1_FEEDBACK_LOG_PATH", "/data/feedback/m1_feedback.jsonl"))
STATE_PATH        = Path(os.environ.get("M1_DAEMON_STATE_PATH", "/data/feedback/daemon_state.json"))
CONFIG_PATH       = os.environ.get("M1_RAY_CONFIG", "/app/training/m1_ray/config_m1_ray.yaml")
BOOTSTRAP_PATH    = os.environ.get("M1_BOOTSTRAP_DATA_PATH", "")
CONTAINER_NAME    = os.environ.get("M1_SERVING_CONTAINER", "m1-serving")
CHECK_INTERVAL    = int(os.environ.get("CHECK_INTERVAL_SECONDS", "300"))
MODEL_NAME        = "m1-ray-categorization"

CORRECTION_THRESHOLD      = 20      # new overrides since last retrain → trigger
ROLLBACK_RATE_DELTA       = 0.15    # 15% worse correction rate → rollback
ROLLBACK_WINDOW_HOURS     = 24      # hours to wait before evaluating rollback
FAILURE_BACKOFF_MINUTES   = 60      # wait this long after a failed retrain before retrying
RELOAD_MAX_RETRIES        = 3       # give up on pending reload after this many attempts

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[DAEMON %(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("retrain_daemon")


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
        "last_correction_count": 0,
        "prev_correction_rate": None,
        "rollback_check_due": None,
        # #6: backoff state for failed retrains
        "consecutive_failures": 0,
        "last_failed_at": None,
        # #9: explicit pre-retrain version for rollback (not version-1 heuristic)
        "version_before_last_retrain": None,
        # P2: pending reload retry state
        "pending_reload_version": None,
        "pending_reload_retries": 0,
    }


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Feedback counting
# ---------------------------------------------------------------------------
def _count_corrections_total() -> int:
    """Count total overrides in the feedback JSONL (all time)."""
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
                if row.get("feedback_type") == "overridden":
                    total += 1
            except json.JSONDecodeError:
                continue
    return total


def _get_correction_rate_for_version(version: str) -> float | None:
    """Ask m1-serving for correction rate since a specific model version."""
    try:
        resp = requests.get(
            f"{M1_SERVING_URL}/metrics/feedback/since/{version}",
            timeout=10,
        )
        if resp.ok:
            data = resp.json()
            return data.get("correction_rate")
    except Exception as e:
        log.warning(f"Could not fetch correction rate for version {version}: {e}")
    return None


# ---------------------------------------------------------------------------
# MLflow helpers
# ---------------------------------------------------------------------------
def _get_latest_version() -> int | None:
    """Return the latest registered version number for m1-ray-categorization."""
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
    """Log a daemon event (trigger, rollback, etc.) to MLflow for audit trail."""
    try:
        import mlflow
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("m1-retrain-daemon")
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
def _run_retrain() -> bool:
    """Run run_retrain_loop.sh and return True on success."""
    script = Path(__file__).parent / "run_retrain_loop.sh"
    if not script.exists():
        log.error(f"retrain script not found: {script}")
        return False

    env = os.environ.copy()
    env["M1_FEEDBACK_INPUT"] = str(FEEDBACK_PATH)
    # #8: shell script reads M1_FEEDBACK_DATASET, not M1_FEEDBACK_OUTPUT
    env["M1_FEEDBACK_DATASET"] = str(FEEDBACK_PATH.parent / "m1_feedback_dataset.csv")
     env["M1_RAY_CONFIG"] = CONFIG_PATH
     if BOOTSTRAP_PATH:
         env["M1_RAY_DATA_PATH"] = BOOTSTRAP_PATH
     env["MLFLOW_TRACKING_URI"] = MLFLOW_URI

    log.info("Starting retraining...")
    try:
        result = subprocess.run(
            ["bash", str(script)],
            env=env,
            capture_output=False,   # let output stream to container logs
            timeout=3600,           # 1 hour max
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
# Hot-reload
# ---------------------------------------------------------------------------
def _trigger_reload() -> bool:
    """POST /admin/reload to m1-serving. Returns True if request succeeded."""
    try:
        resp = requests.post(f"{M1_SERVING_URL}/admin/reload", timeout=10)
        if resp.ok:
            log.info(f"Reload triggered: {resp.json()}")
            return True
        log.warning(f"Reload returned {resp.status_code}: {resp.text}")
    except Exception as e:
        log.warning(f"Reload request failed: {e}")
    return False


def _verify_deployed_version(expected_version: str, wait_seconds: int = 15) -> bool:
    """Poll /health until model_version matches expected_version or timeout.

    The reload happens in a background thread inside the serving process, so
    we need to wait for it to complete before we can confirm the version.
    Returns True only when /health reports the exact expected version.
    """
    import time as _time
    deadline = _time.monotonic() + wait_seconds
    while _time.monotonic() < deadline:
        try:
            resp = requests.get(f"{M1_SERVING_URL}/health", timeout=5)
            if resp.ok:
                data = resp.json()
                actual = str(data.get("model_version", ""))
                if actual == str(expected_version):
                    log.info(f"Confirmed deployed version: v{actual}")
                    return True
                if data.get("status") == "degraded":
                    log.warning("Serving reports degraded — model load failed")
                    return False
        except Exception as e:
            log.warning(f"Health check failed during version verification: {e}")
        _time.sleep(3)

    log.warning(
        f"Version verification timed out after {wait_seconds}s — "
        f"expected v{expected_version}"
    )
    return False


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
def _rollback_to_version(version: int) -> None:
    """Roll back m1-serving to a specific model version.

    Uses POST /admin/reload?version=<N> which sets M1_MODEL_VERSION inside
    the serving process before calling real_model.load(). This is safe,
    zero-downtime, and guaranteed to load exactly the requested version
    because _select_model_version() respects M1_MODEL_VERSION env var.

    No container restart needed — the env var is set in the serving process
    memory via the reload endpoint.
    """
    log.warning(f"Rolling back to version {version}...")
    try:
        resp = requests.post(
            f"{M1_SERVING_URL}/admin/reload",
            params={"version": str(version)},
            timeout=15,
        )
        if resp.ok:
            data = resp.json()
            log.info(f"Rollback reload triggered: {data}")
            # Give the reload thread time to complete
            time.sleep(10)

            # Verify rollback actually loaded the right version
            health = requests.get(f"{M1_SERVING_URL}/health", timeout=5).json()
            loaded = health.get("model_version")
            if str(loaded) == str(version):
                log.info(f"Rollback confirmed: serving version {loaded}")
            else:
                log.error(
                    f"Rollback verification FAILED: requested v{version}, "
                    f"serving reports v{loaded}"
                )

            _log_event_to_mlflow("rollback", {
                "rollback_to_version": str(version),
                "loaded_version": str(loaded),
                "reason": "correction_rate_degraded",
                "verified": str(loaded) == str(version),
            })
        else:
            log.error(f"Rollback reload returned {resp.status_code}: {resp.text}")
            log.error(
                f"Manual rollback: POST {M1_SERVING_URL}/admin/reload?version={version}"
            )
    except Exception as e:
        log.error(f"Rollback failed: {e}")
        log.error(
            f"Manual rollback: POST {M1_SERVING_URL}/admin/reload?version={version}"
        )


# ---------------------------------------------------------------------------
# Weekly trigger check
# ---------------------------------------------------------------------------
def _should_weekly_trigger(state: dict) -> bool:
    """Return True if it is Sunday and > 6 days since last retrain."""
    now = datetime.now(timezone.utc)
    if now.weekday() != 6:   # 6 = Sunday
        return False
    if state["last_retrain_at"] is None:
        return True
    last = datetime.fromisoformat(state["last_retrain_at"])
    return (now - last).days >= 6


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def run():
    log.info(f"Retrain daemon starting | model={MODEL_NAME} | mlflow={MLFLOW_URI}")
    log.info(f"Feedback path: {FEEDBACK_PATH}")
    log.info(f"Check interval: {CHECK_INTERVAL}s | correction threshold: {CORRECTION_THRESHOLD}")

    while True:
        try:
            state = _load_state()
            now_iso = datetime.now(timezone.utc).isoformat()

            # ----------------------------------------------------------------
            # 1. Check rollback window first (don't retrain during evaluation)
            # ----------------------------------------------------------------
            if state.get("rollback_check_due"):
                due = datetime.fromisoformat(state["rollback_check_due"])
                if datetime.now(timezone.utc) >= due:
                    log.info("Rollback evaluation window reached — checking correction rate...")
                    last_version = state.get("last_retrain_version")
                    prev_rate = state.get("prev_correction_rate")

                    if last_version and prev_rate is not None:
                        current_rate = _get_correction_rate_for_version(str(last_version))
                        if current_rate is not None:
                            log.info(f"Correction rate: prev={prev_rate:.3f} new_model={current_rate:.3f}")
                            if current_rate > prev_rate * (1 + ROLLBACK_RATE_DELTA):
                                log.warning(
                                    f"Correction rate degraded {prev_rate:.1%} → {current_rate:.1%} "
                                    f"(threshold {ROLLBACK_RATE_DELTA:.0%}). Rolling back."
                                )
                                # #9: use explicitly stored pre-retrain version, not version-1 heuristic
                                rollback_target = state.get("version_before_last_retrain")
                                if rollback_target is None:
                                    # Legacy state: fall back to version-1 but warn
                                    rollback_target = str(int(last_version) - 1)
                                    log.warning(
                                        f"version_before_last_retrain not in state, "
                                        f"using heuristic v{rollback_target}"
                                    )
                                if int(rollback_target) >= 1:
                                    _rollback_to_version(int(rollback_target))
                            else:
                                log.info("Correction rate acceptable — keeping new model.")
                                _log_event_to_mlflow("rollback_check_passed", {
                                    "version": last_version,
                                    "prev_rate": prev_rate,
                                    "current_rate": current_rate,
                                })

                    state["rollback_check_due"] = None
                    _save_state(state)

            # ----------------------------------------------------------------
            # 1b. Retry pending reload (P2 fix)
            # A previous retrain registered a new version but the reload failed.
            # Retry until confirmed or max retries reached.
            # Corrections are preserved (watermark not advanced) until reload succeeds.
            # ----------------------------------------------------------------
            if state.get("pending_reload_version"):
                pending_ver = state["pending_reload_version"]
                retries = state.get("pending_reload_retries", 0)
                log.info(
                    f"Retrying pending reload v{pending_ver} "
                    f"(attempt {retries + 1}/{RELOAD_MAX_RETRIES})"
                )
                retry_ok = _trigger_reload()
                time.sleep(8)

                # Verify /health confirms the version, not just that the request succeeded
                retry_verified = False
                if retry_ok:
                    retry_verified = _verify_deployed_version(pending_ver, wait_seconds=30)

                if retry_ok and retry_verified:
                    log.info(f"Pending reload v{pending_ver} confirmed on retry {retries + 1}")
                    # Now safe to advance watermark and record version as live
                    current_corrections = _count_corrections_total()
                    state["last_correction_count"] = current_corrections
                    state["last_retrain_version"] = pending_ver
                    state["pending_reload_version"] = None
                    state["pending_reload_retries"] = 0
                    _save_state(state)
                    _log_event_to_mlflow("reload_retry_succeeded", {
                        "version": pending_ver,
                        "attempts": str(retries + 1),
                        "verified": "true",
                    })
                else:
                    retries += 1
                    state["pending_reload_retries"] = retries
                    if retries >= RELOAD_MAX_RETRIES:
                        log.error(
                            f"Pending reload v{pending_ver} failed after "
                            f"{RELOAD_MAX_RETRIES} attempts — giving up. "
                            "Advancing watermark to prevent infinite correction loop."
                        )
                        # Give up — advance watermark so corrections aren't retried forever
                        current_corrections = _count_corrections_total()
                        state["last_correction_count"] = current_corrections
                        state["pending_reload_version"] = None
                        state["pending_reload_retries"] = 0
                        _log_event_to_mlflow("reload_permanently_failed", {
                            "version": pending_ver,
                            "attempts": str(retries),
                        })
                    else:
                        log.warning(
                            f"Reload retry failed ({retries}/{RELOAD_MAX_RETRIES}) "
                            f"— will retry next cycle"
                        )
                    _save_state(state)
                # Don't process corrections until pending reload is resolved or given up
                time.sleep(CHECK_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # 2. Check retrain triggers
            # ----------------------------------------------------------------
            current_corrections = _count_corrections_total()
            last_corrections = state.get("last_correction_count", 0)
            new_corrections = current_corrections - last_corrections

            weekly = _should_weekly_trigger(state)
            correction_trigger = new_corrections >= CORRECTION_THRESHOLD

            if not correction_trigger and not weekly:
                log.info(
                    f"No trigger: new_corrections={new_corrections}/{CORRECTION_THRESHOLD}, "
                    f"weekly={weekly}"
                )
                time.sleep(CHECK_INTERVAL)
                continue

            trigger_reason = "weekly_schedule" if weekly else f"corrections={new_corrections}"
            log.info(f"RETRAIN TRIGGERED: {trigger_reason}")

            # #6: check backoff — don't hammer if previous attempt recently failed
            consecutive_failures = state.get("consecutive_failures", 0)
            last_failed_at = state.get("last_failed_at")
            if consecutive_failures > 0 and last_failed_at:
                backoff_until = datetime.fromisoformat(last_failed_at) + timedelta(
                    minutes=FAILURE_BACKOFF_MINUTES * consecutive_failures
                )
                if datetime.now(timezone.utc) < backoff_until:
                    log.warning(
                        f"Skipping retrain — in backoff after {consecutive_failures} "
                        f"consecutive failure(s). Retry after {backoff_until.isoformat()}"
                    )
                    time.sleep(CHECK_INTERVAL)
                    continue

            # #9: record current version BEFORE retrain for explicit rollback target
            version_before = _get_latest_version()
            prev_rate = _get_correction_rate_for_version(
                str(version_before)
            ) if version_before else None

            # ----------------------------------------------------------------
            # 3. Run retraining
            # ----------------------------------------------------------------
            retrain_ok = _run_retrain()

            if not retrain_ok:
                # #6: failed retrain — do NOT advance watermark (corrections
                # are still unprocessed and should trigger retry after backoff).
                consecutive_failures = state.get("consecutive_failures", 0) + 1
                state["consecutive_failures"] = consecutive_failures
                state["last_failed_at"] = now_iso
                state["last_retrain_at"] = now_iso  # prevent weekly re-trigger
                _save_state(state)
                log.error(
                    f"Retrain failed (consecutive_failures={consecutive_failures}). "
                    f"Corrections preserved for retry after "
                    f"{FAILURE_BACKOFF_MINUTES * consecutive_failures}min backoff."
                )
                _log_event_to_mlflow("retrain_failed", {
                    "trigger": trigger_reason,
                    "version_before": str(version_before),
                    "consecutive_failures": str(consecutive_failures),
                })
                time.sleep(CHECK_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # 4. Check if a new version was registered
            # ----------------------------------------------------------------
            version_after = _get_latest_version()
            if version_after is None or version_after == version_before:
                log.info(
                    f"Retrain ran but no new version registered "
                    f"(version_before={version_before}, version_after={version_after}). "
                    "Model did not improve — not reloading."
                )
                _log_event_to_mlflow("retrain_no_improvement", {
                    "trigger": trigger_reason,
                    "version": str(version_before),
                    "new_corrections": str(new_corrections),
                })
                state["last_correction_count"] = current_corrections
                state["last_retrain_at"] = now_iso
                state["consecutive_failures"] = 0  # reset — retrain ran OK, just no improvement
                state["last_failed_at"] = None
                _save_state(state)
                time.sleep(CHECK_INTERVAL)
                continue

            log.info(f"New version registered: v{version_before} → v{version_after}")

            # ----------------------------------------------------------------
            # 5. Hot-reload serving container
            # ----------------------------------------------------------------
            # #3: trigger reload and verify serving actually loaded the new version
            reload_ok = _trigger_reload()

            # Wait for the background reload thread, then confirm via /health
            # that model_version == version_after before recording it as live.
            verified = False
            if reload_ok:
                verified = _verify_deployed_version(str(version_after), wait_seconds=30)

            if not reload_ok or not verified:
                log.error(
                    f"Reload {'request failed' if not reload_ok else 'succeeded but version not confirmed'} "
                    f"for v{version_after}. "
                    "Corrections preserved — will retry next cycle."
                )
                # P2 fix: do NOT advance last_correction_count.
                # Store pending version so retry loop handles it next cycle.
                state["last_retrain_at"] = now_iso
                state["consecutive_failures"] = 0
                state["last_failed_at"] = None
                state["pending_reload_version"] = str(version_after)
                state["pending_reload_retries"] = 0
                state["version_before_last_retrain"] = str(version_before) if version_before else None
                _save_state(state)
                _log_event_to_mlflow("reload_failed", {
                    "trigger": trigger_reason,
                    "version_before": str(version_before),
                    "version_after": str(version_after),
                    "reload_request_ok": str(reload_ok),
                    "version_verified": str(verified),
                })
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
            state["last_correction_count"] = current_corrections
            state["prev_correction_rate"] = prev_rate
            state["rollback_check_due"] = rollback_due
            state["consecutive_failures"] = 0
            state["last_failed_at"] = None
            # #9: store explicit pre-retrain version for rollback (not version-1 heuristic)
            state["version_before_last_retrain"] = str(version_before) if version_before else None
            _save_state(state)

            _log_event_to_mlflow("retrain_success", {
                "trigger": trigger_reason,
                "version_before": str(version_before),
                "version_after": str(version_after),
                "new_corrections": str(new_corrections),
                "prev_correction_rate": str(prev_rate),
                "rollback_check_due": rollback_due,
            })

            log.info(
                f"Done. v{version_after} loaded and confirmed. "
                f"Rollback check scheduled at {rollback_due}"
            )

        except Exception as e:
            log.exception(f"Unexpected error in daemon loop: {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run()
