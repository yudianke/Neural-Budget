FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY model.onnx .
COPY gunicorn.conf.py .

EXPOSE 8003

ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc_m2

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:app"]
