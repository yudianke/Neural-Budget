FROM python:3.11-slim

WORKDIR /app
COPY requirements_m2.txt .
RUN pip install --no-cache-dir -r requirements_m2.txt

COPY train_m2.py .
COPY config_m2.yaml .

ENTRYPOINT ["python", "train_m2.py"]
