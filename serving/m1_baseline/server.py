from fastapi import FastAPI
from schemas import M1Input, M1Output
from mock_model import predict_category

app = FastAPI(
    title="NeuralBudget M1 Baseline Service",
    version="1.0.0",
    description="Baseline FastAPI service for M1 transaction auto-categorization."
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M1 Baseline Service",
        "version": "1.0.0",
        "endpoints": ["/health", "/predict/category"]
    }


@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    return predict_category(request)
