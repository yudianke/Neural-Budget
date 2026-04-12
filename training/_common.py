"""Shared utilities for M1/M2/M3 training scripts."""
import os
import subprocess
import time
import warnings
from pathlib import Path

import mlflow
import pandas as pd
import yaml
from mlflow.tracking import MlflowClient

os.environ["GIT_PYTHON_REFRESH"] = "quiet"
warnings.filterwarnings("ignore")

CACHE_DIR = Path(os.environ.get("DATA_CACHE_DIR", "/tmp/nb_data_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def log(prefix, msg):
    print(f"[{prefix} {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def resolve_path(path, prefix="M"):
    """Local path or swift:// URL -> local path. Caches downloads."""
    if not path.startswith("swift://"):
        return path
    _, rest = path.split("swift://", 1)
    container, object_name = rest.split("/", 1)
    local_path = CACHE_DIR / container / object_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if not local_path.exists():
        log(prefix, f"downloading {path}")
        subprocess.run(
            ["swift", "download", container, object_name, "-o", str(local_path)],
            check=True,
        )
    else:
        log(prefix, f"using cached {local_path}")
    return str(local_path)


def setup_mlflow(config):
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", config.get("mlflow_tracking_uri"))
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(config["experiment_name"])
    return tracking_uri


def get_latest_production_metric(model_name, metric_name):
    """Pull the metric from the latest registered version of a model.
    Returns (version_number, metric_value) or (None, None) if no version exists.
    """
    try:
        client = MlflowClient()
        try:
            client.get_registered_model(model_name)
        except Exception:
            return None, None
        versions = client.search_model_versions(f"name='{model_name}'")
        if not versions:
            return None, None
        versions = sorted(versions, key=lambda v: int(v.version), reverse=True)
        latest = versions[0]
        run = client.get_run(latest.run_id)
        metric = run.data.metrics.get(metric_name)
        return int(latest.version), metric
    except Exception as e:
        print(f"[WARN] could not fetch latest metric for {model_name}: {e}")
        return None, None


def should_register(mode, current_metric, previous_metric, higher_is_better, absolute_gate_passed, prefix="M"):
    """Decide whether to register a new model version.
    Returns (should_register: bool, reason: str).
    """
    if not absolute_gate_passed:
        return False, f"absolute gate failed (current={current_metric:.4f})"

    if mode == "bootstrap":
        return True, f"bootstrap mode, absolute gate passed (current={current_metric:.4f})"

    if previous_metric is None:
        return True, f"retrain mode but no previous version, registering (current={current_metric:.4f})"

    if higher_is_better:
        improved = current_metric > previous_metric
        delta = current_metric - previous_metric
    else:
        improved = current_metric < previous_metric
        delta = previous_metric - current_metric

    if improved:
        return (
            True,
            f"improved over previous (prev={previous_metric:.4f} -> curr={current_metric:.4f}, delta=+{abs(delta):.4f})",
        )
    return (
        False,
        f"no improvement over previous (prev={previous_metric:.4f} -> curr={current_metric:.4f}, delta=-{abs(delta):.4f})",
    )


def register_model_version(client, model_name, run_id, artifact_path):
    """Create or get the registered model, then create a new version pointing at the run."""
    try:
        client.get_registered_model(model_name)
    except Exception:
        client.create_registered_model(model_name)
    model_uri = f"runs:/{run_id}/{artifact_path}"
    return client.create_model_version(name=model_name, source=model_uri, run_id=run_id)


def load_and_combine(bootstrap_path, production_path, prefix="M"):
    """Load bootstrap CSV. If production_path provided and exists, load and concatenate."""
    log(prefix, f"loading bootstrap {bootstrap_path}")
    boot_local = resolve_path(bootstrap_path, prefix)
    boot_df = pd.read_csv(boot_local)
    log(prefix, f"bootstrap rows: {len(boot_df):,}")

    if not production_path:
        return boot_df

    log(prefix, f"loading production {production_path}")
    try:
        prod_local = resolve_path(production_path, prefix)
        prod_df = pd.read_csv(prod_local)
        log(prefix, f"production rows: {len(prod_df):,}")
        combined = pd.concat([boot_df, prod_df], ignore_index=True)
        log(prefix, f"combined rows: {len(combined):,}")
        return combined
    except Exception as e:
        log(prefix, f"production data not available ({e}), using bootstrap only")
        return boot_df
