from contextlib import asynccontextmanager

from fastapi import FastAPI

import real_model
from schemas import M1FeedbackBatch, M1Input, M1Output

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

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class CrossOriginMiddleware(BaseHTTPMiddleware):
    """Handle CORS + COEP manually to ensure full cross-origin compatibility.

    Browsers with COEP: require-corp need Cross-Origin-Resource-Policy on
    every response AND proper CORS headers (some browsers reject wildcard '*'
    for Access-Control-Allow-Origin under COEP).
    """

    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "*")

        # Handle CORS preflight (OPTIONS)
        if request.method == "OPTIONS":
            return Response(
                content="OK",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Max-Age": "600",
                    "Cross-Origin-Resource-Policy": "cross-origin",
                },
            )

        response: Response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
        return response


app.add_middleware(CrossOriginMiddleware)


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
        "model": real_model.get_model_info(),
        "endpoints": ["/health", "/predict/category", "/feedback"],
    }


@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    print(
        f"[M1-REQ] merchant={request.merchant!r} amount={request.amount} "
        f"txn_id={request.transaction_id}"
    )
    result = real_model.predict(request)
    print(
        f"[M1-RES] predicted={result.predicted_category} "
        f"confidence={result.confidence:.2f} "
        f"top3={[(s.category, round(s.confidence, 2)) for s in result.top_3_suggestions]}"
    )
    return result


@app.post("/feedback")
def feedback_endpoint(request: M1FeedbackBatch):
    return {"logged": real_model.log_feedback(request.entries)}
