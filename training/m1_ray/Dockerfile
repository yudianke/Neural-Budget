FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ git bash \
    && rm -rf /var/lib/apt/lists/*

# Install daemon dependencies (copied from m1_ray subdir)
COPY m1_ray/requirements-daemon.txt .
RUN pip install --no-cache-dir -r requirements-daemon.txt

# Copy shared training utilities
COPY _common.py /app/training/_common.py

# Copy m1_ray training code
COPY m1_ray/ /app/training/m1_ray/

WORKDIR /app/training/m1_ray

CMD ["python", "retrain_daemon.py"]
