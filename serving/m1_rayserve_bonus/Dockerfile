FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serve_app.py .
COPY model.onnx .

CMD ["sh", "-c", "ray start --head --dashboard-host=0.0.0.0 && serve run serve_app:deployment"]
