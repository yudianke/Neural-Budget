"""
M2 Feedback Dataset Builder
============================
Transforms raw M2 feedback JSONL (from the serving /feedback endpoint)
into a structured CSV used by the retrain daemon to decide threshold
adjustments and contamination tuning.

Unlike M1 (supervised — feedback provides correct labels), M2 feedback
is one-sided: dismiss_false_positive means the user disagrees with the
anomaly flag.  This does NOT produce labeled training data for
IsolationForest (which is unsupervised).  Instead, it produces:

  - Aggregate statistics: dismiss_rate, per-rule dismiss rates
  - A cleaned CSV of dismissed transactions (features + context)
    that the retrain script uses to:
      * Evaluate whether contamination should be lowered
      * Identify which rules produce the most false positives
      * Exclude known-good transactions from anomaly seeding

Usage:
  python build_feedback_dataset.py --input /data/feedback/m2_feedback.jsonl \
                                    --output /data/feedback/m2_feedback_dataset.csv
"""

import argparse
import json
from pathlib import Path

import pandas as pd


def load_feedback(path: Path) -> pd.DataFrame:
    if path.suffix == ".jsonl":
        rows = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return pd.DataFrame(rows)
    return pd.read_csv(path)


def main():
    parser = argparse.ArgumentParser(description="Build M2 feedback dataset from JSONL logs")
    parser.add_argument("--input", required=True, help="Path to feedback JSONL file")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = load_feedback(input_path)
    if df.empty:
        raise SystemExit("No feedback rows found")

    # Normalize columns
    keep = [
        "transaction_id",
        "feedback_type",
        "badge_type",
        "anomaly_score",
        "merchant",
        "amount",
        "date",
        "logged_at",
        "model_name",
        "model_version",
    ]
    for col in keep:
        if col not in df.columns:
            df[col] = None

    # Extract rule_flags into separate columns if present
    if "rule_flags" in df.columns:
        def extract_flag(row, flag):
            rf = row.get("rule_flags")
            if isinstance(rf, dict):
                return rf.get(flag, False)
            return False

        df["flag_duplicate"] = df.apply(lambda r: extract_flag(r, "duplicate_within_24h"), axis=1)
        df["flag_subscription_jump"] = df.apply(lambda r: extract_flag(r, "subscription_jump"), axis=1)
        df["flag_amount_spike"] = df.apply(lambda r: extract_flag(r, "amount_spike"), axis=1)
        keep.extend(["flag_duplicate", "flag_subscription_jump", "flag_amount_spike"])

    out = df[keep].copy()
    out["anomaly_score"] = pd.to_numeric(out["anomaly_score"], errors="coerce")
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")

    # Deduplicate: keep last feedback per transaction
    out = out.sort_values("logged_at", kind="stable").drop_duplicates(
        subset=["transaction_id", "feedback_type"],
        keep="last",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    # Print summary statistics for the retrain daemon to parse
    total = len(out)
    dismissals = len(out[out["feedback_type"] == "dismiss_false_positive"])
    confirms = len(out[out["feedback_type"] == "confirmed_anomaly"])
    dismiss_rate = dismissals / total if total > 0 else 0.0

    print(f"Total feedback entries: {total}")
    print(f"Dismissals: {dismissals}")
    print(f"Confirms: {confirms}")
    print(f"Dismiss rate: {dismiss_rate:.4f}")

    if "badge_type" in out.columns:
        per_rule = out[out["feedback_type"] == "dismiss_false_positive"].groupby("badge_type").size()
        print(f"Per-rule dismissals:\n{per_rule.to_string()}")

    print(f"Saved feedback dataset to {output_path}")


if __name__ == "__main__":
    main()
