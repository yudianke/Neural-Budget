FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serve_app.py .
COPY model.onnx .

CMD ["serve", "run", "serve_app:deployment", "--host", "0.0.0.0", "--port", "8003"]
