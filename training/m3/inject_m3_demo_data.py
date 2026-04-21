#!/usr/bin/env python3
"""
inject_m3_demo_data.py
======================
Sends realistic forecast requests to the M3 service to demonstrate
the forecasting feature with multiple categories and months.

Usage:
    python training/m3/inject_m3_demo_data.py --base-url http://129.114.27.248:8002
    python training/m3/inject_m3_demo_data.py --base-url http://localhost:8002
"""

import argparse
import json
import math
import sys
import time
from datetime import date

import requests


# Realistic monthly spend profiles per category
CATEGORY_PROFILES = [
    {
        "name": "Groceries",
        "monthly_spends": [420.0, 445.0, 398.0, 462.0, 431.0, 418.0],
        "std": 25.0,
    },
    {
        "name": "Restaurants",
        "monthly_spends": [185.0, 210.0, 175.0, 230.0, 195.0, 205.0],
        "std": 18.0,
    },
    {
        "name": "Utilities",
        "monthly_spends": [142.0, 138.0, 145.0, 141.0, 139.0, 143.0],
        "std": 8.0,
    },
    {
        "name": "Transport",
        "monthly_spends": [95.0, 112.0, 88.0, 105.0, 98.0, 115.0],
        "std": 12.0,
    },
    {
        "name": "Entertainment",
        "monthly_spends": [78.0, 95.0, 65.0, 110.0, 82.0, 90.0],
        "std": 15.0,
    },
    {
        "name": "Healthcare",
        "monthly_spends": [45.0, 120.0, 35.0, 55.0, 40.0, 200.0],
        "std": 60.0,
    },
    {
        "name": "Shopping",
        "monthly_spends": [230.0, 185.0, 310.0, 195.0, 420.0, 275.0],
        "std": 85.0,
    },
    {
        "name": "Housing",
        "monthly_spends": [1500.0, 1500.0, 1500.0, 1500.0, 1500.0, 1500.0],
        "std": 0.0,
    },
]


def build_feature_row(category_name: str, spends: list, month_num: int, year: int) -> dict:
    """Build a feature row from 6 months of spend history."""
    lag_1 = spends[-1]
    lag_2 = spends[-2]
    lag_3 = spends[-3]
    lag_6 = spends[0]

    rolling_mean_3 = sum(spends[-3:]) / 3
    rolling_std_3 = (sum((x - rolling_mean_3) ** 2 for x in spends[-3:]) / 3) ** 0.5
    rolling_mean_6 = sum(spends) / 6
    rolling_std_6 = (sum((x - rolling_mean_6) ** 2 for x in spends) / 6) ** 0.5

    quarter = math.ceil(month_num / 3)
    month_sin = math.sin(2 * math.pi * month_num / 12)
    month_cos = math.cos(2 * math.pi * month_num / 12)
    is_q4 = 1 if month_num in (10, 11, 12) else 0

    return {
        "project_category": category_name,
        "monthly_spend": lag_1,
        "lag_1": lag_1,
        "lag_2": lag_2,
        "lag_3": lag_3,
        "lag_4": spends[-4] if len(spends) >= 4 else lag_3,
        "lag_5": spends[-5] if len(spends) >= 5 else lag_3,
        "lag_6": lag_6,
        "rolling_mean_3": round(rolling_mean_3, 2),
        "rolling_std_3": round(rolling_std_3, 2),
        "rolling_mean_6": round(rolling_mean_6, 2),
        "rolling_std_6": round(rolling_std_6, 2),
        "rolling_max_3": max(spends[-3:]),
        "history_month_count": float(len(spends)),
        "month_num": float(month_num),
        "quarter": float(quarter),
        "year": float(year),
        "is_q4": float(is_q4),
        "month_sin": round(month_sin, 6),
        "month_cos": round(month_cos, 6),
        "user_total_lag_1": sum(p["monthly_spends"][-1] for p in CATEGORY_PROFILES),
        "user_total_rolling_mean_3": sum(
            sum(p["monthly_spends"][-3:]) / 3 for p in CATEGORY_PROFILES
        ),
        "category_share_lag_1": round(
            lag_1 / max(sum(p["monthly_spends"][-1] for p in CATEGORY_PROFILES), 1), 4
        ),
        "budgeted": lag_1 * 1.05,  # simulate budget as 5% above last month
    }


def run(base_url: str, pretty: bool = False) -> int:
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    # Health check
    print(f"Connecting to M3 service at {base_url}...")
    try:
        r = session.get(f"{base_url}/health", timeout=10)
        r.raise_for_status()
        health = r.json()
        print(f"M3 health: {health}")
    except Exception as e:
        print(f"ERROR: M3 service not reachable: {e}", file=sys.stderr)
        return 1

    today = date.today()
    month_num = today.month
    year = today.year

    # Build feature rows for all categories
    rows = []
    for profile in CATEGORY_PROFILES:
        row = build_feature_row(
            category_name=profile["name"],
            spends=profile["monthly_spends"],
            month_num=month_num,
            year=year,
        )
        rows.append(row)

    print(f"\nSending forecast request for {len(rows)} categories...")

    started = time.perf_counter()
    try:
        r = session.post(
            f"{base_url}/forecast/features",
            data=json.dumps({"rows": rows}),
            timeout=15,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        r.raise_for_status()
        result = r.json()
    except Exception as e:
        print(f"ERROR: Forecast request failed: {e}", file=sys.stderr)
        return 1

    print(f"Latency: {elapsed_ms:.1f}ms")
    print(f"Model: {result.get('model_name', 'unknown')}")
    print(f"\n{'Category':<20} {'Last Month':>12} {'Forecast':>12} {'Gap':>10}")
    print("-" * 58)

    forecasts = result.get("forecasts", [])
    for f in forecasts:
        cat = f.get("category", "?")
        forecast = f.get("forecast", 0) or 0
        # Find matching profile for last month spend
        last_month = next(
            (p["monthly_spends"][-1] for p in CATEGORY_PROFILES if p["name"] == cat),
            0,
        )
        gap = forecast - last_month
        gap_str = f"+${gap:.2f}" if gap >= 0 else f"-${abs(gap):.2f}"
        print(f"{cat:<20} ${last_month:>10.2f} ${forecast:>10.2f} {gap_str:>10}")

    print(f"\nTotal categories forecast: {len(forecasts)}")

    if pretty:
        print("\nFull response:")
        print(json.dumps(result, indent=2))

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inject demo data into M3 forecast service")
    parser.add_argument("--base-url", default="http://localhost:8002", help="M3 service URL")
    parser.add_argument("--pretty", action="store_true", help="Print full JSON response")
    args = parser.parse_args()
    return run(args.base_url, args.pretty)


if __name__ == "__main__":
    raise SystemExit(main())
