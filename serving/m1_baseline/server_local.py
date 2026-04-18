"""Local dev server using mock_model — no MLflow/XGBoost needed."""
from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

import mock_model
from schemas import M1FeedbackBatch, M1Input, M1Output

app = FastAPI(title="NeuralBudget M1 (local mock)", version="1.0.0")


class CrossOriginMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin", "*")
        if request.method == "OPTIONS":
            return Response(
                content="OK",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
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
    return {"status": "ok", "mode": "local-mock"}


@app.get("/")
def root():
    return {"service": "NeuralBudget M1 (local mock)", "endpoints": ["/health", "/predict/category", "/feedback"]}


@app.post("/predict/category", response_model=M1Output)
def predict_category_endpoint(request: M1Input):
    result = mock_model.predict_category(request)
    print(f"[M1-local] merchant={request.merchant!r} -> {result.predicted_category} ({result.confidence:.0%})")
    return result


@app.post("/feedback")
def feedback_endpoint(request: M1FeedbackBatch):
    for e in request.entries:
        print(f"[M1-feedback] {e.merchant!r} predicted={e.predicted_category} chosen={e.chosen_category} type={e.feedback_type}")
    return {"logged": len(request.entries)}
