# NeuralBudget M1 Baseline

This folder contains the baseline FastAPI serving implementation for the M1 transaction auto-categorization subsystem.

## Files

- `schemas.py`: request/response schema definitions
- `mock_model.py`: baseline rule-based category predictor
- `server.py`: FastAPI app entrypoint
- `Dockerfile`: container image for Chameleon deployment
- `requirements.txt`: Python dependencies

## Endpoint

- `GET /health`
- `POST /predict/category`
