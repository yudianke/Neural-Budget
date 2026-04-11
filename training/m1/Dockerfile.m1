FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY train_m1.py .
COPY config_m1.yaml .

ENTRYPOINT ["python", "train_m1.py"]
