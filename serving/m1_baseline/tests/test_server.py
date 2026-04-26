"""Tests for FastAPI server endpoints."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from schemas import CategorySuggestion, M1Output


@pytest.fixture
def app_client(mock_real_model):
    """Create a test client with mocked real_model."""
    with patch.dict("sys.modules", {"real_model": mock_real_model}):
        mock_offline_eval = MagicMock()
        with patch.dict("sys.modules", {"offline_eval": mock_offline_eval}):
            mock_real_model.predict.return_value = M1Output(
                transaction_id="test1",
                synthetic_user_id="user_1",
                predicted_category="restaurants",
                confidence=0.95,
                top_3_suggestions=[
                    CategorySuggestion(category="restaurants", confidence=0.95),
                    CategorySuggestion(category="groceries", confidence=0.03),
                    CategorySuggestion(category="misc", confidence=0.02),
                ],
                auto_fill=True,
            )
            mock_real_model.log_feedback.return_value = 1

            from prometheus_client import REGISTRY

            for collector in list(REGISTRY._collector_to_names.keys()):
                names = REGISTRY._collector_to_names.get(collector, set())
                if any(name.startswith("m1_") for name in names):
                    REGISTRY.unregister(collector)

            if "server" in sys.modules:
                del sys.modules["server"]

            import importlib

            server_mod = importlib.import_module("server")
            server_mod._model_ready = True

            from starlette.testclient import TestClient

            client = TestClient(server_mod.app)
            yield client


class TestHealthEndpoint:
    def test_health_ok(self, app_client):
        resp = app_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["model_version"] == "7"
        assert data["eval_gate_passed"] is True


class TestRootEndpoint:
    def test_root_returns_service_info(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "NeuralBudget M1 Baseline Service"
        assert "/metrics/evaluation" in data["endpoints"]
        assert "/admin/run-eval" in data["endpoints"]


class TestPredictEndpoint:
    def test_predict_valid_input(self, app_client, sample_m1_input):
        resp = app_client.post("/predict/category", json=sample_m1_input)
        assert resp.status_code == 200
        data = resp.json()
        assert data["predicted_category"] == "restaurants"
        assert data["confidence"] == 0.95
        assert data["auto_fill"] is True
        assert len(data["top_3_suggestions"]) == 3

    def test_predict_missing_field_returns_422(self, app_client):
        resp = app_client.post("/predict/category", json={"transaction_id": "t1"})
        assert resp.status_code == 422


class TestEvaluationEndpoint:
    def test_evaluation_metrics(self, app_client):
        resp = app_client.get("/metrics/evaluation")
        assert resp.status_code == 200
        data = resp.json()
        assert data["gate_passed"] is True
        assert "evaluations" in data
        assert "sanity" in data["evaluations"]


class TestFeedbackEndpoint:
    def test_feedback_accepted(self, app_client):
        payload = {
            "entries": [
                {
                    "transaction_id": "t1",
                    "date": "2024-01-01",
                    "amount": -10.0,
                    "merchant": "SUBWAY",
                    "predicted_category": "restaurants",
                    "chosen_category": "restaurants",
                    "confidence": 0.95,
                    "feedback_type": "accepted",
                    "top_3_suggestions": [
                        {"category": "restaurants", "confidence": 0.95}
                    ],
                }
            ]
        }
        resp = app_client.post("/feedback", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["logged"] == 1
