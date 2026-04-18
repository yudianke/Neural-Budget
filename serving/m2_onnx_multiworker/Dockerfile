FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY model.onnx .
COPY gunicorn.conf.py .

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:app"]
