from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import real_model
from schemas import M1Input, M1Output

_model_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model_ready
    real_model.load()
    _model_ready = True
    yield


app = FastAPI(
    title="NeuralBudget M1 Baseline Service",
    version="1.0.0",
    description="Baseline FastAPI service for M1 transaction auto-categorization.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    if not _model_ready:
        return {"status": "loading"}
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "service": "NeuralBudget M1 Baseline Service",
        "version": "1.0.0",
        "endpoints": ["/health", "/predict/category"],
    }


@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    return real_model.predict(request)
