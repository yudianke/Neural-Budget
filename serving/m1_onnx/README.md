# NeuralBudget M1 ONNX Service

This directory contains the ONNX Runtime CPU serving option for the M1 transaction auto-categorization subsystem.

Files:
- `export_to_onnx.py`: exports a tiny M1-compatible model to `model.onnx`
- `app.py`: FastAPI inference service using ONNX Runtime
- `Dockerfile`: container definition for the ONNX CPU serving option
- `requirements.txt`: Python dependencies
