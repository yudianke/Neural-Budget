import json
from pathlib import Path
import numpy as np
import pandas as pd


def load_config(config_path: str = "data_pipeline/manifest.json") -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def normalize_merchant(merchant: str) -> str:
    if merchant is None:
        return "UNKNOWN"
    return str(merchant).strip().upper()


def compute_online_features(
    incoming_txn: dict,
    historical_txns: pd.DataFrame,
) -> dict:
    """
    Online inference features for one incoming transaction
    Uses only historical transactions prior to the current event
    """

    user_id = incoming_txn["synthetic_user_id"]
    merchant = normalize_merchant(incoming_txn["merchant"])
    amount = float(incoming_txn["amount"])
    abs_amount = abs(amount)
    txn_date = pd.to_datetime(incoming_txn["date"])
    transaction_type = incoming_txn["transaction_type"]

    # Restrict to this user's prior history only
    hist = historical_txns.copy()
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist[
        (hist["synthetic_user_id"] == user_id) &
        (hist["date"] < txn_date)
    ].copy()

    # Merchant-level prior behavior
    merchant_hist = hist[hist["merchant"].astype(str).str.upper() == merchant].copy()

    merchant_prior_count = len(merchant_hist)

    if merchant_prior_count > 0:
        merchant_majority_category = (
            merchant_hist["project_category"]
            .mode()
            .iloc[0]
        )
        merchant_mean_abs_amount = merchant_hist["amount"].abs().mean()
    else:
        merchant_majority_category = None
        merchant_mean_abs_amount = 0.0

    # User-level priors
    user_prior_count = len(hist)

    if user_prior_count > 0:
        user_mean_abs_amount = hist["amount"].abs().mean()
        user_std_abs_amount = hist["amount"].abs().std()
        if pd.isna(user_std_abs_amount):
            user_std_abs_amount = 0.0
    else:
        user_mean_abs_amount = 0.0
        user_std_abs_amount = 0.0

    # Rolling 30-day window
    hist_30d = hist[hist["date"] >= (txn_date - pd.Timedelta(days=30))].copy()
    rolling_30d_mean = hist_30d["amount"].abs().mean() if len(hist_30d) > 0 else 0.0
    rolling_30d_std = hist_30d["amount"].abs().std() if len(hist_30d) > 0 else 0.0
    if pd.isna(rolling_30d_std):
        rolling_30d_std = 0.0

    # Repeat count based on merchant + rounded amount
    rounded_abs_amount = round(abs_amount, 2)
    repeat_count = len(
        hist[
            (hist["merchant"].astype(str).str.upper() == merchant) &
            (hist["amount"].abs().round(2) == rounded_abs_amount)
        ]
    )

    is_recurring_candidate = int(repeat_count >= 2)

    # Z-score against user history
    if user_std_abs_amount > 0:
        amount_zscore = (abs_amount - user_mean_abs_amount) / user_std_abs_amount
    else:
        amount_zscore = 0.0

    feature_payload = {
        "synthetic_user_id": user_id,
        "date": str(txn_date),
        "merchant": merchant,
        "transaction_type": transaction_type,
        "amount": amount,
        "abs_amount": round(abs_amount, 2),
        "log_abs_amount": round(float(np.log1p(abs_amount)), 4),
        "day_of_week": int(txn_date.dayofweek),
        "day_of_month": int(txn_date.day),
        "month": int(txn_date.month),
        "user_prior_count": int(user_prior_count),
        "merchant_prior_count": int(merchant_prior_count),
        "merchant_majority_category": merchant_majority_category,
        "merchant_mean_abs_amount": round(float(merchant_mean_abs_amount), 2),
        "user_mean_abs_amount": round(float(user_mean_abs_amount), 2),
        "user_std_abs_amount": round(float(user_std_abs_amount), 2),
        "rolling_30d_mean": round(float(rolling_30d_mean), 2),
        "rolling_30d_std": round(float(rolling_30d_std), 2),
        "repeat_count": int(repeat_count),
        "is_recurring_candidate": int(is_recurring_candidate),
        "amount_zscore": round(float(amount_zscore), 4),
    }

    return feature_payload


def main() -> None:
    config = load_config()
    processed_dir = Path(config["output_dir"])

    txns_path = processed_dir / "synthetic_transactions.csv"
    txns = pd.read_csv(txns_path)

    txns["date"] = pd.to_datetime(txns["date"], errors="coerce")
    txns = txns.dropna(subset=["date"]).copy()

    # Pick one realistic incoming event from later in time for demo
    txns = txns.sort_values(["synthetic_user_id", "date"]).reset_index(drop=True)

    # Choose a row not too early so history exists
    candidate_idx = min(5000, len(txns) - 1)
    incoming_row = txns.iloc[candidate_idx].to_dict()

    incoming_txn = {
        "synthetic_user_id": incoming_row["synthetic_user_id"],
        "date": str(pd.to_datetime(incoming_row["date"])),
        "merchant": incoming_row["merchant"],
        "transaction_type": incoming_row["transaction_type"],
        "amount": float(incoming_row["amount"]),
    }

    feature_payload = compute_online_features(incoming_txn, txns)

    output_path = processed_dir / "online_feature_sample.json"
    with open(output_path, "w") as f:
        json.dump(feature_payload, f, indent=2)

    print("Incoming transaction:")
    print(json.dumps(incoming_txn, indent=2))
    print("\nComputed online features:")
    print(json.dumps(feature_payload, indent=2))
    print(f"\nSaved {output_path}")


if __name__ == "__main__":
    main()