import argparse
import pandas as pd
import requests
import json
import time
from pathlib import Path



def load_transactions(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df = df.sort_values(["synthetic_user_id", "date"]).reset_index(drop=True)
    return df


def build_payload(row: pd.Series) -> dict:
    return {
        "transaction_id": row["transaction_id"],
        "synthetic_user_id": row["synthetic_user_id"],
        "date": str(pd.to_datetime(row["date"])),
        "merchant": row["merchant"],
        "transaction_type": row["transaction_type"],
        "amount": float(row["amount"]),
        "project_category": row["project_category"],
        "persona_cluster": int(row["persona_cluster"]) if "persona_cluster" in row and pd.notna(row["persona_cluster"]) else None,
    }


def send_request(endpoint: str, payload: dict, timeout: int = 10) -> dict:
    response = requests.post(endpoint, json=payload, timeout=timeout)
    response.raise_for_status()
    try:
        return response.json()
    except Exception:
        return {"status_code": response.status_code, "text": response.text}


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate production transaction events.")
    parser.add_argument(
        "--input",
        default="data/processed/synthetic_transactions.csv",
        help="Path to synthetic transaction CSV",
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8000/infer",
        help="Service endpoint to send events to",
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=20,
        help="Number of events to send",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.2,
        help="Delay between requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not send requests; just print payloads",
    )
    parser.add_argument(
        "--output-log",
        default="data/processed/generator_log.jsonl",
        help="Path to save request/response log",
    )

    args = parser.parse_args()

    df = load_transactions(args.input)
    if len(df) == 0:
        raise ValueError("No transactions found in input CSV.")

    sample_df = df.head(args.num_events).copy()

    output_log = Path(args.output_log)
    output_log.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loaded {len(df)} transactions from {args.input}")
    print(f"Using first {len(sample_df)} events for simulation")
    print(f"Endpoint: {args.endpoint}")
    print(f"Dry run: {args.dry_run}")

    with open(output_log, "w") as log_file:
        for idx, (_, row) in enumerate(sample_df.iterrows(), start=1):
            payload = build_payload(row)

            if args.dry_run:
                result = {
                    "mode": "dry_run",
                    "event_index": idx,
                    "payload": payload,
                }
                print(f"\n[DRY RUN] Event {idx}")
                print(json.dumps(payload, indent=2))
            else:
                try:
                    response_json = send_request(args.endpoint, payload)
                    result = {
                        "mode": "live",
                        "event_index": idx,
                        "payload": payload,
                        "response": response_json,
                    }
                    print(f"\n[LIVE] Event {idx} sent successfully")
                    print(json.dumps({"payload": payload, "response": response_json}, indent=2))
                except Exception as e:
                    result = {
                        "mode": "live",
                        "event_index": idx,
                        "payload": payload,
                        "error": str(e),
                    }
                    print(f"\n[ERROR] Event {idx}")
                    print(json.dumps({"payload": payload, "error": str(e)}, indent=2))

            log_file.write(json.dumps(result) + "\n")
            time.sleep(args.sleep_seconds)

    print(f"\nSaved generator log to {output_log}")


if __name__ == "__main__":
    main()


