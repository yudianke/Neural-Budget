import argparse
import dataclasses
import json
import math
import random
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

import requests


@dataclasses.dataclass(frozen=True)
class CategoryProfile:
    category_id: str
    category_name: str
    monthly_budget: float
    mean_amount: float
    std_amount: float
    min_amount: float
    max_amount: float
    monthly_txn_range: tuple[int, int]
    merchants: list[str]
    seasonality_weight: dict[int, float]


DEFAULT_CATEGORY_PROFILES = [
    CategoryProfile(
        category_id="food",
        category_name="Food",
        monthly_budget=550.0,
        mean_amount=22.0,
        std_amount=8.0,
        min_amount=7.0,
        max_amount=65.0,
        monthly_txn_range=(12, 22),
        merchants=["Chipotle", "Trader Joe's", "Whole Foods", "Target Grocery"],
        seasonality_weight={},
    ),
    CategoryProfile(
        category_id="internet",
        category_name="Internet",
        monthly_budget=80.0,
        mean_amount=78.0,
        std_amount=3.0,
        min_amount=65.0,
        max_amount=95.0,
        monthly_txn_range=(1, 2),
        merchants=["Verizon", "Spectrum"],
        seasonality_weight={},
    ),
    CategoryProfile(
        category_id="entertainment",
        category_name="Entertainment",
        monthly_budget=180.0,
        mean_amount=28.0,
        std_amount=15.0,
        min_amount=8.0,
        max_amount=95.0,
        monthly_txn_range=(3, 8),
        merchants=["AMC", "Spotify", "Netflix", "Steam", "Apple"],
        seasonality_weight={6: 1.15, 7: 1.2, 11: 1.25, 12: 1.35},
    ),
]


class ProductionDataSimulator:
    def __init__(
        self,
        base_url: str,
        user_id: str,
        months: int,
        seed: int,
        categories: list[CategoryProfile],
        timeout_seconds: float = 10.0,
        sleep_ms: int = 0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_id = user_id
        self.months = months
        self.random = random.Random(seed)
        self.categories = categories
        self.timeout_seconds = timeout_seconds
        self.sleep_ms = sleep_ms

        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        self.ingest_url = f"{self.base_url}/transactions/ingest"
        self.forecast_url = f"{self.base_url}/forecast/features"
        self.health_url = f"{self.base_url}/health"

        self._stats: dict[str, Any] = {
            "transactions_sent": 0,
            "transactions_ok": 0,
            "transactions_failed": 0,
            "forecast_requests": 0,
            "forecast_ok": 0,
            "forecast_failed": 0,
            "by_category_count": Counter(),
            "by_category_spend": defaultdict(float),
            "monthly_spend": defaultdict(float),
            "latencies_ms": [],
            "failures": [],
        }

    def run(self, include_forecast_requests: bool) -> dict[str, Any]:
        self._check_health()

        month_list = self._generate_months()
        tx_counter = 0

        for ym in month_list:
            year, month = map(int, ym.split("-"))

            for category in self.categories:
                txns = self._generate_month_category_transactions(
                    year=year,
                    month=month,
                    category=category,
                    tx_counter_start=tx_counter,
                )
                tx_counter += len(txns)

                for txn in txns:
                    self._send_transaction(txn)

                    if include_forecast_requests and self._should_request_forecast():
                        self._send_forecast_request(ym)

                    if self.sleep_ms > 0:
                        time.sleep(self.sleep_ms / 1000.0)

        summary = self._build_summary()
        return summary

    def _check_health(self) -> None:
        response = self.session.get(self.health_url, timeout=self.timeout_seconds)
        response.raise_for_status()

    def _generate_months(self) -> list[str]:
        today = date.today()
        months: list[str] = []
        year = today.year
        month = today.month

        for _ in range(self.months):
            months.append(f"{year:04d}-{month:02d}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        months.reverse()
        return months

    def _days_in_month(self, year: int, month: int) -> int:
        if month == 12:
            next_month = date(year + 1, 1, 1)
        else:
            next_month = date(year, month + 1, 1)
        this_month = date(year, month, 1)
        return (next_month - this_month).days

    def _month_weight(self, category: CategoryProfile, month: int) -> float:
        return category.seasonality_weight.get(month, 1.0)

    def _generate_month_category_transactions(
        self,
        year: int,
        month: int,
        category: CategoryProfile,
        tx_counter_start: int,
    ) -> list[dict[str, Any]]:
        days_in_month = self._days_in_month(year, month)
        seasonal = self._month_weight(category, month)
        txn_count = self.random.randint(*category.monthly_txn_range)

        txns: list[dict[str, Any]] = []
        for i in range(txn_count):
            amount = self.random.gauss(category.mean_amount, category.std_amount)
            amount *= seasonal
            amount = max(category.min_amount, min(amount, category.max_amount))
            amount = round(amount, 2)

            day = self.random.randint(1, days_in_month)
            merchant = self.random.choice(category.merchants)

            txns.append(
                {
                    "user_id": self.user_id,
                    "transaction_id": f"{self.user_id}-{year}{month:02d}-{tx_counter_start + i + 1:05d}",
                    "date": f"{year:04d}-{month:02d}-{day:02d}",
                    "category_id": category.category_id,
                    "category_name": category.category_name,
                    "amount": -amount,
                    "payee": merchant,
                }
            )

        return txns

    def _send_transaction(self, payload: dict[str, Any]) -> None:
        self._stats["transactions_sent"] += 1
        started = time.perf_counter()

        try:
            response = self.session.post(
                self.ingest_url,
                data=json.dumps(payload),
                timeout=self.timeout_seconds,
            )
            elapsed_ms = (time.perf_counter() - started) * 1000
            self._stats["latencies_ms"].append(elapsed_ms)

            response.raise_for_status()
            body = response.json()
            if body.get("status") != "ok":
                raise RuntimeError(f"Unexpected ingest response: {body}")

            self._stats["transactions_ok"] += 1
            self._stats["by_category_count"][payload["category_name"]] += 1
            self._stats["by_category_spend"][payload["category_name"]] += abs(payload["amount"])
            self._stats["monthly_spend"][payload["date"][:7]] += abs(payload["amount"])

        except Exception as exc:
            self._stats["transactions_failed"] += 1
            self._stats["failures"].append(
                {
                    "endpoint": "/transactions/ingest",
                    "transaction_id": payload.get("transaction_id"),
                    "error": str(exc),
                }
            )

    def _should_request_forecast(self) -> bool:
        return self.random.random() < 0.08

    def _send_forecast_request(self, year_month: str) -> None:
        month_num = int(year_month[5:7])
        year = int(year_month[:4])
        quarter = math.floor((month_num - 1) / 3) + 1
        month_sin = math.sin(2 * math.pi * month_num / 12)
        month_cos = math.cos(2 * math.pi * month_num / 12)

        rows = []
        for category in self.categories:
            rows.append(
                {
                    "project_category": category.category_name,
                    "monthly_spend": category.mean_amount * 8,
                    "lag_1": category.mean_amount * 7,
                    "lag_2": category.mean_amount * 7,
                    "lag_3": category.mean_amount * 6,
                    "lag_6": category.mean_amount * 6,
                    "rolling_mean_3": category.mean_amount * 7,
                    "rolling_std_3": max(category.std_amount, 1.0),
                    "rolling_mean_6": category.mean_amount * 6.5,
                    "rolling_max_3": category.max_amount,
                    "history_month_count": float(max(self.months, 1)),
                    "month_num": float(month_num),
                    "quarter": float(quarter),
                    "year": float(year),
                    "is_q4": float(1 if month_num in (10, 11, 12) else 0),
                    "user_total_lag_1": 800.0,
                    "user_total_rolling_mean_3": 780.0,
                    "category_share_lag_1": min((category.mean_amount * 7) / 800.0, 1.0),
                }
            )

        payload = {"rows": rows}
        self._stats["forecast_requests"] += 1

        try:
            response = self.session.post(
                self.forecast_url,
                data=json.dumps(payload),
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            _ = response.json()
            self._stats["forecast_ok"] += 1
        except Exception as exc:
            self._stats["forecast_failed"] += 1
            self._stats["failures"].append(
                {
                    "endpoint": "/forecast/features",
                    "year_month": year_month,
                    "error": str(exc),
                }
            )

    def _build_summary(self) -> dict[str, Any]:
        latency_ms = self._stats["latencies_ms"]
        p50_ms = statistics.median(latency_ms) if latency_ms else 0.0
        p95_ms = (
            sorted(latency_ms)[max(int(len(latency_ms) * 0.95) - 1, 0)]
            if latency_ms
            else 0.0
        )

        return {
            "user_id": self.user_id,
            "months_simulated": self.months,
            "transactions_sent": self._stats["transactions_sent"],
            "transactions_ok": self._stats["transactions_ok"],
            "transactions_failed": self._stats["transactions_failed"],
            "forecast_requests": self._stats["forecast_requests"],
            "forecast_ok": self._stats["forecast_ok"],
            "forecast_failed": self._stats["forecast_failed"],
            "latency_ms_p50": round(p50_ms, 2),
            "latency_ms_p95": round(p95_ms, 2),
            "spend_by_category": {
                k: round(v, 2) for k, v in sorted(self._stats["by_category_spend"].items())
            },
            "monthly_spend": {
                k: round(v, 2) for k, v in sorted(self._stats["monthly_spend"].items())
            },
            "failures": self._stats["failures"],
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate production-like local transaction traffic for M3."
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8002",
        help="Base URL for the M3 service.",
    )
    parser.add_argument(
        "--user-id",
        default="local-user",
        help="User ID to simulate.",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Number of months of historical traffic to simulate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--include-forecast-requests",
        action="store_true",
        help="Also send occasional forecast requests during transaction simulation.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Optional delay between transaction requests.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the final summary JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    simulator = ProductionDataSimulator(
        base_url=args.base_url,
        user_id=args.user_id,
        months=args.months,
        seed=args.seed,
        categories=DEFAULT_CATEGORY_PROFILES,
        sleep_ms=args.sleep_ms,
    )

    try:
        summary = simulator.run(include_forecast_requests=args.include_forecast_requests)
    except requests.RequestException as exc:
        print(f"[ERROR] Failed to communicate with M3 service: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"[ERROR] Simulation failed: {exc}", file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(summary, indent=2))
    else:
        print(json.dumps(summary))

    if summary["transactions_failed"] > 0 or summary["forecast_failed"] > 0:
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())