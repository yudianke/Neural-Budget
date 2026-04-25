"""Unit tests for offline_eval.py — no MLflow or live model required."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from offline_eval import (
    EvalResults,
    SANITY_CASES,
    _compute_eval_results,
    _normalize_merchant,
)


class TestNormalizeMerchant:
    """Verify offline_eval's _normalize_merchant matches real_model's."""

    def test_strips_digits(self):
        assert _normalize_merchant("SUBWAY 26377") == "SUBWAY"

    def test_upper(self):
        assert _normalize_merchant("subway") == "SUBWAY"

    def test_none(self):
        assert _normalize_merchant(None) == ""


class TestEvalResults:
    def test_to_dict_round_trips(self):
        er = EvalResults(
            source="test",
            accuracy=0.8567,
            macro_f1=0.7234,
            weighted_f1=0.8,
            per_category_f1={"restaurants": 0.95, "misc": 0.32},
            underperforming=["misc=0.320"],
            eval_size=100,
            gate_passed=True,
            gate_floor=0.7,
            gate_reason="macro_f1=0.7234 >= 0.7",
            timestamp="2024-01-01T00:00:00Z",
            model_version="7",
            duration_ms=42.5,
        )
        d = er.to_dict()
        assert d["accuracy"] == 0.8567
        assert d["macro_f1"] == 0.7234
        assert d["per_category_f1"]["restaurants"] == 0.95
        assert d["gate_passed"] is True
        assert d["eval_size"] == 100


class TestComputeEvalResults:
    def test_perfect_predictions(self):
        y_true = np.array([0, 1, 2, 0, 1])
        y_pred = np.array([0, 1, 2, 0, 1])

        # Mock label encoder
        le = MagicMock()
        le.inverse_transform = lambda x: [["cat_a", "cat_b", "cat_c"][i] for i in x]

        import time
        result = _compute_eval_results(
            y_true, y_pred, le, "test", 0.5, "7", time.time()
        )
        assert result.accuracy == 1.0
        assert result.macro_f1 == 1.0
        assert result.gate_passed is True
        assert len(result.confusion_top5) == 0

    def test_poor_predictions_fail_gate(self):
        y_true = np.array([0, 0, 0, 1, 1])
        y_pred = np.array([1, 1, 1, 0, 0])  # all wrong

        le = MagicMock()
        le.inverse_transform = lambda x: [["cat_a", "cat_b"][i] for i in x]

        import time
        result = _compute_eval_results(
            y_true, y_pred, le, "test", 0.5, "7", time.time()
        )
        assert result.accuracy == 0.0
        assert result.gate_passed is False
        assert len(result.confusion_top5) > 0

    def test_partial_predictions(self):
        y_true = np.array([0, 0, 1, 1, 0])
        y_pred = np.array([0, 1, 1, 1, 0])  # 1 wrong

        le = MagicMock()
        le.inverse_transform = lambda x: [["cat_a", "cat_b"][i] for i in x]

        import time
        result = _compute_eval_results(
            y_true, y_pred, le, "test", 0.5, "7", time.time()
        )
        assert 0.5 < result.accuracy < 1.0
        assert result.gate_passed is True


class TestSanityCases:
    def test_sanity_cases_not_empty(self):
        assert len(SANITY_CASES) > 10

    def test_sanity_cases_have_valid_format(self):
        for merchant, category, description in SANITY_CASES:
            assert isinstance(merchant, str)
            assert isinstance(category, str)
            assert isinstance(description, str)
            assert len(merchant) > 0
            assert len(category) > 0
