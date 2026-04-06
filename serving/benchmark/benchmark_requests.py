import json
import time
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed


def send_one(url, payload):
    start = time.time()
    try:
        r = requests.post(url, json=payload, timeout=10)
        latency_ms = (time.time() - start) * 1000
        return {
            "status_code": r.status_code,
            "latency_ms": latency_ms,
            "ok": r.status_code == 200,
        }
    except Exception:
        latency_ms = (time.time() - start) * 1000
        return {
            "status_code": None,
            "latency_ms": latency_ms,
            "ok": False,
        }


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    idx = int(len(sorted_vals) * p)
    idx = min(max(idx, 0), len(sorted_vals) - 1)
    return sorted_vals[idx]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        payload = json.load(f)

    latencies = []
    errors = 0

    start_all = time.time()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(send_one, args.url, payload) for _ in range(args.requests)]

        for fut in as_completed(futures):
            result = fut.result()
            latencies.append(result["latency_ms"])
            if not result["ok"]:
                errors += 1

    total_time = time.time() - start_all
    latencies.sort()

    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)
    throughput = args.requests / total_time if total_time > 0 else 0.0
    error_rate = errors / args.requests if args.requests > 0 else 0.0

    print("===== Benchmark Result =====")
    print(f"Total requests: {args.requests}")
    print(f"Concurrency: {args.concurrency}")
    print(f"p50 latency (ms): {p50:.2f}")
    print(f"p95 latency (ms): {p95:.2f}")
    print(f"Throughput (req/s): {throughput:.2f}")
    print(f"Error rate: {error_rate:.4f}")


if __name__ == "__main__":
    main()
