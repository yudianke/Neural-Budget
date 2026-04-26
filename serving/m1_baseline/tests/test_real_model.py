"""Unit tests for real_model.py — no MLflow or live model required."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from real_model import _build_row, _null_prediction, map_category, normalize_merchant
from schemas import M1Input


class TestNormalizeMerchant:
    def test_basic_uppercase(self):
        assert normalize_merchant("subway") == "SUBWAY"

    def test_strips_long_digits(self):
        assert normalize_merchant("SUBWAY 26377 CORNE") == "SUBWAY CORNE"

    def test_keeps_short_digits(self):
        assert normalize_merchant("ROUTE 66 CAFE") == "ROUTE 66 CAFE"

    def test_collapses_whitespace(self):
        assert normalize_merchant("UBER   *TRIP") == "UBER *TRIP"

    def test_strips_leading_trailing(self):
        assert normalize_merchant("  TESCO  ") == "TESCO"

    def test_none_input(self):
        assert normalize_merchant(None) == ""

    def test_empty_string(self):
        assert normalize_merchant("") == ""

    def test_numeric_only(self):
        assert normalize_merchant("123456") == ""

    def test_mixed_case_preserved_as_upper(self):
        assert normalize_merchant("McDonald's") == "MCDONALD'S"


class TestMapCategory:
    def test_canonical_passthrough(self):
        for cat in ["restaurants", "groceries", "shopping", "utilities", "misc"]:
            assert map_category(cat) == cat

    def test_legacy_mapping(self):
        assert map_category("Dine Out") == "restaurants"
        assert map_category("Groceries") == "groceries"
        assert map_category("Amazon") == "shopping"
        assert map_category("Bills") == "utilities"
        assert map_category("Mortgage") == "housing"
        assert map_category("Travel") == "transport"

    def test_trailing_space(self):
        assert map_category("Groceries ") == "groceries"

    def test_unknown_falls_to_misc(self):
        assert map_category("NONEXISTENT_CATEGORY") == "misc"

    def test_empty_string(self):
        assert map_category("") == "misc"

    def test_canonical_with_whitespace(self):
        assert map_category(" restaurants ") == "restaurants"


class TestBuildRow:
    def test_builds_correct_dataframe(self):
        inp = M1Input(
            transaction_id="t1",
            synthetic_user_id="u1",
            date="2024-01-15",
            merchant="SUBWAY 26377",
            amount=-10.0,
            transaction_type="DEB",
            account_type="checking",
            day_of_week=0,
            day_of_month=15,
            month=1,
            log_abs_amount=2.397,
            historical_majority_category_for_payee="restaurants",
        )
        row, merchant_clean = _build_row(inp)
        assert merchant_clean == "SUBWAY"
        assert len(row) == 1
        assert row.iloc[0]["merchant"] == "SUBWAY"
        assert row.iloc[0]["log_amount"] == 2.397
        assert row.iloc[0]["day_of_week"] == 0
        assert row.iloc[0]["day_of_month"] == 15


class TestNullPrediction:
    def test_returns_correct_schema(self):
        inp = M1Input(
            transaction_id="t1",
            synthetic_user_id="u1",
            date="2024-01-15",
            merchant="TEST",
            amount=-10.0,
            transaction_type="DEB",
            account_type="checking",
            day_of_week=0,
            day_of_month=15,
            month=1,
            log_abs_amount=2.397,
            historical_majority_category_for_payee="",
        )
        result = _null_prediction(inp)
        assert result.transaction_id == "t1"
        assert result.confidence == 0.0
        assert result.predicted_category == ""
        assert result.auto_fill is False
        assert result.top_3_suggestions == []
