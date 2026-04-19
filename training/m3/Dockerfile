FROM python:3.10-slim

WORKDIR /app

COPY training/m3/requirements_m3.txt /app/requirements_m3.txt
RUN pip install --no-cache-dir -r /app/requirements_m3.txt

COPY training/m3 /app

EXPOSE 8002

CMD ["uvicorn", "m3_inference_service:app", "--host", "0.0.0.0", "--port", "8002"]