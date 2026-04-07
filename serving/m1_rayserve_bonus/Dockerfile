FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serve_app.py .
COPY run_serve.py .
COPY model.onnx .

CMD ["sh", "-c", "ray start --head --disable-usage-stats && python run_serve.py"]
