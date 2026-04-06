from fastapi import FastAPI
from app.schemas import M1Input, M1Output
from app.mock_model import predict_category

app = FastAPI(title="NeuralBudget M1 Serving API", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict/category", response_model=M1Output)
def predict(payload: M1Input):
    return predict_category(payload)
