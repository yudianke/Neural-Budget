FROM python:3.11-slim

WORKDIR /app
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
COPY requirements_m3.txt .
RUN pip install --no-cache-dir -r requirements_m3.txt

COPY train_m3.py .
COPY config_m3.yaml .

ENTRYPOINT ["python", "train_m3.py"]
