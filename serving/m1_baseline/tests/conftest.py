"""Shared fixtures for M1 serving tests."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def mock_real_model():
    """Provide a mock real_model module for tests that don't need MLflow."""
    mock = MagicMock()
    mock._model_version = "7"
    mock._use_fallback = False
    mock._fallback_mode = None
    mock._last_eval_results = {
        "enabled": True,
        "gate_passed": True,
        "reason": "sanity: macro_f1=1.0000 >= 0.7",
        "model_version": "7",
        "evaluations": {
            "sanity": {
                "source": "sanity",
                "accuracy": 1.0,
                "macro_f1": 1.0,
                "weighted_f1": 1.0,
                "per_category_f1": {"restaurants": 1.0, "groceries": 1.0},
                "underperforming_categories": [],
                "confusion_top5": [],
                "eval_size": 17,
                "gate_passed": True,
                "gate_floor": 0.7,
                "gate_reason": "macro_f1=1.0000 >= 0.7",
            },
        },
    }
    mock.get_model_info.return_value = {
        "model_name": "m1-ray-categorization",
        "model_version": "7",
        "class_count": 13,
        "mode": "ray-xgboost",
    }
    mock.get_eval_results.return_value = mock._last_eval_results
    mock.get_feedback_stats.return_value = {
        "total": 3,
        "corrections": 1,
        "correction_rate": 0.333,
        "current_version": "7",
        "filter_version": None,
    }
    return mock


@pytest.fixture
def sample_m1_input():
    """A valid M1Input dict for testing."""
    return {
        "transaction_id": "test1",
        "synthetic_user_id": "user_1",
        "date": "2024-01-01",
        "merchant": "SUBWAY 26377 CORNE",
        "amount": -1.32,
        "transaction_type": "DEB",
        "account_type": "checking",
        "day_of_week": 0,
        "day_of_month": 1,
        "month": 1,
        "log_abs_amount": 0.84,
        "historical_majority_category_for_payee": "restaurants",
    }
