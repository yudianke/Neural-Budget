import os
import shutil

workers = 4
worker_class = "uvicorn.workers.UvicornWorker"
bind = "0.0.0.0:8003"
timeout = 60

# ---------------------------------------------------------------------------
# Prometheus multiprocess support
# Each worker writes its metrics to a file in PROMETHEUS_MULTIPROC_DIR.
# The /metrics endpoint uses MultiProcessCollector to aggregate all files.
# ---------------------------------------------------------------------------
_prom_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc_m2")
os.makedirs(_prom_dir, exist_ok=True)


def child_exit(server, worker):
    """Clean up this worker's metric files when it exits."""
    from prometheus_client import multiprocess  # noqa: PLC0415
    multiprocess.mark_process_dead(worker.pid)
