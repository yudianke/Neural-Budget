import argparse
import json
from pathlib import Path

import pandas as pd


def load_feedback(path: Path) -> pd.DataFrame:
    if path.suffix == ".jsonl":
        rows = []
        with path.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return pd.DataFrame(rows)
    return pd.read_csv(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to feedback jsonl/csv")
    parser.add_argument("--output", required=True, help="Path to normalized csv")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Optional filter to keep only feedback above this model confidence",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    df = load_feedback(input_path)
    if df.empty:
        raise SystemExit("No feedback rows found")

    if "confidence" in df.columns:
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce").fillna(0.0)
        df = df[df["confidence"] >= args.min_confidence].copy()

    if "chosen_category" not in df.columns and "category" in df.columns:
        df["chosen_category"] = df["category"]

    if "merchant" not in df.columns:
        merchant = None
        for candidate in ["payee_name", "imported_payee"]:
            if candidate in df.columns:
                series = df[candidate].astype(str).str.strip()
                merchant = series if merchant is None else merchant.mask(merchant == "", series)
        df["merchant"] = merchant if merchant is not None else ""

    keep = [
        "date",
        "merchant",
        "amount",
        "chosen_category",
        "predicted_category",
        "feedback_type",
        "confidence",
        "transaction_id",
        "logged_at",
        "model_name",
        "model_version",
    ]
    for column in keep:
        if column not in df.columns:
            df[column] = None

    out = df[keep].rename(columns={"chosen_category": "category"}).copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    # Fall back to logged_at if date is missing, then today as last resort
    if "logged_at" in out.columns:
        fallback = pd.to_datetime(out["logged_at"], errors="coerce")
        out["date"] = out["date"].fillna(fallback)
    out["date"] = out["date"].fillna(pd.Timestamp.now())
    out["merchant"] = out["merchant"].astype(str).str.strip()
    out["category"] = out["category"].astype(str).str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce").fillna(-10.0)
    out = out[(out["merchant"] != "") & (out["category"] != "")]
    out = out.sort_values("logged_at", kind="stable").drop_duplicates(
        subset=["date", "merchant", "amount", "category", "feedback_type"],
        keep="last",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    print(f"Input rows: {len(df):,}")
    print(f"Output rows: {len(out):,}")
    print(f"Saved normalized feedback dataset to {output_path}")


if __name__ == "__main__":
    main()
