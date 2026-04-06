import pandas as pd
import json
from pathlib import Path



def dataset_summary(df: pd.DataFrame, name: str) -> dict:
    if "date" in df.columns:
        range_info = {
            "min": str(df["date"].min()),
            "max": str(df["date"].max()),
        }
    elif "year_month" in df.columns:
        range_info = {
            "min": str(df["year_month"].min()),
            "max": str(df["year_month"].max()),
        }
    else:
        range_info = {
            "min": None,
            "max": None,
        }

    return {
        "name": name,
        "num_rows": len(df),
        "num_users": df["synthetic_user_id"].nunique() if "synthetic_user_id" in df.columns else None,
        "date_range": range_info,
    }


def load_config(config_path: str = "data_pipeline/manifest.json") -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    print(f"Saved {path}")


def main() -> None:
    config = load_config()

    processed_dir = Path(config["output_dir"])
    batch_dir = processed_dir / "batch_datasets"
    ensure_dir(batch_dir)

    txns_path = processed_dir / "synthetic_transactions.csv"
    users_path = processed_dir / "synthetic_users.csv"

    txns = pd.read_csv(txns_path)
    users = pd.read_csv(users_path)

    txns["date"] = pd.to_datetime(txns["date"], errors="coerce")
    txns = txns.dropna(subset=["date"]).copy()


    # Categorization dataset

    categorization_cols = [
        "transaction_id",
        "synthetic_user_id",
        "persona_cluster",
        "date",
        "merchant",
        "transaction_type",
        "amount",
        "abs_amount",
        "day_of_week",
        "day_of_month",
        "month",
        "log_abs_amount",
        "project_category",
        "repeat_count",
        "is_recurring_candidate",
    ]
    categorization_cols = [c for c in categorization_cols if c in txns.columns]

    categorization_df = txns[categorization_cols].copy()

    # Chronological split to avoid leakage
    categorization_df = categorization_df.sort_values("date").reset_index(drop=True)
    split_idx = int(len(categorization_df) * 0.8)

    categorization_train = categorization_df.iloc[:split_idx].copy()
    categorization_eval = categorization_df.iloc[split_idx:].copy()

    save_csv(categorization_train, batch_dir / "categorization_train.csv")
    save_csv(categorization_eval, batch_dir / "categorization_eval.csv")


    #Anomaly dataset

    anomaly_df = txns.copy()

    #Candidate selection: only users with sufficient transaction history
    user_counts = anomaly_df.groupby("synthetic_user_id").size().reset_index(name="txn_count")
    eligible_users = user_counts[user_counts["txn_count"] >= 20]["synthetic_user_id"]

    anomaly_df = anomaly_df[anomaly_df["synthetic_user_id"].isin(eligible_users)].copy()
    anomaly_df = anomaly_df.sort_values(["synthetic_user_id", "date"]).reset_index(drop=True)

    #Build rolling historical features using only prior records
    anomaly_df["user_txn_index"] = anomaly_df.groupby("synthetic_user_id").cumcount()

    anomaly_df["user_mean_abs_amount_prior"] = (
        anomaly_df.groupby("synthetic_user_id")["abs_amount"]
        .expanding()
        .mean()
        .shift(1)
        .reset_index(level=0, drop=True)
    )

    anomaly_df["user_std_abs_amount_prior"] = (
        anomaly_df.groupby("synthetic_user_id")["abs_amount"]
        .expanding()
        .std()
        .shift(1)
        .reset_index(level=0, drop=True)
    )

    anomaly_df["user_mean_abs_amount_prior"] = anomaly_df["user_mean_abs_amount_prior"].fillna(0)
    anomaly_df["user_std_abs_amount_prior"] = anomaly_df["user_std_abs_amount_prior"].fillna(0)

    anomaly_cols = [
        "transaction_id",
        "synthetic_user_id",
        "persona_cluster",
        "date",
        "merchant",
        "project_category",
        "amount",
        "abs_amount",
        "repeat_count",
        "is_recurring_candidate",
        "user_txn_index",
        "user_mean_abs_amount_prior",
        "user_std_abs_amount_prior",
    ]
    anomaly_cols = [c for c in anomaly_cols if c in anomaly_df.columns]
    anomaly_df = anomaly_df[anomaly_cols].copy()

    anomaly_df = anomaly_df.sort_values("date").reset_index(drop=True)
    split_idx = int(len(anomaly_df) * 0.8)

    anomaly_train = anomaly_df.iloc[:split_idx].copy()
    anomaly_eval = anomaly_df.iloc[split_idx:].copy()

    save_csv(anomaly_train, batch_dir / "anomaly_train.csv")
    save_csv(anomaly_eval, batch_dir / "anomaly_eval.csv")


    #Forecasting dataset

    forecasting_df = txns.copy()
    forecasting_df["year_month"] = forecasting_df["date"].dt.to_period("M").astype(str)

    monthly_spend = (
        forecasting_df.groupby(["synthetic_user_id", "year_month", "project_category"], as_index=False)["abs_amount"]
        .sum()
        .rename(columns={"abs_amount": "monthly_spend"})
    )

    monthly_spend = monthly_spend.sort_values(["synthetic_user_id", "year_month", "project_category"]).reset_index(drop=True)

    # Split by time, not random
    unique_months = sorted(monthly_spend["year_month"].unique().tolist())
    split_month_idx = max(1, int(len(unique_months) * 0.8))
    train_months = set(unique_months[:split_month_idx])
    eval_months = set(unique_months[split_month_idx:])

    forecasting_train = monthly_spend[monthly_spend["year_month"].isin(train_months)].copy()
    forecasting_eval = monthly_spend[monthly_spend["year_month"].isin(eval_months)].copy()

    save_csv(forecasting_train, batch_dir / "forecasting_train.csv")
    save_csv(forecasting_eval, batch_dir / "forecasting_eval.csv")


    #Save batch manifest

    batch_manifest = {
        "source_files": {
            "synthetic_transactions": str(txns_path),
            "synthetic_users": str(users_path),
        },
        "selection_rules": {
            "categorization": "All transactions with required fields; chronological 80/20 split.",
            "anomaly": "Only users with >= 20 transactions; rolling history features computed from prior transactions only; chronological 80/20 split.",
            "forecasting": "Monthly aggregation by user and category; split by month to avoid future leakage.",
        },
        "leakage_prevention": [
            "No random split for time-dependent data.",
            "Chronological split used for categorization and anomaly datasets.",
            "Forecasting split uses earlier months for training and later months for evaluation.",
            "Anomaly historical features are computed using prior transactions only."
        ],
        "dataset_summaries": [
            dataset_summary(categorization_train, "categorization_train"),
            dataset_summary(categorization_eval, "categorization_eval"),
            dataset_summary(anomaly_train, "anomaly_train"),
            dataset_summary(anomaly_eval, "anomaly_eval"),
            dataset_summary(forecasting_train, "forecasting_train"),
            dataset_summary(forecasting_eval, "forecasting_eval"),
        ],

        "outputs": [
            "categorization_train.csv",
            "categorization_eval.csv",
            "anomaly_train.csv",
            "anomaly_eval.csv",
            "forecasting_train.csv",
            "forecasting_eval.csv"
        ],
    }

    with open(batch_dir / "batch_manifest.json", "w") as f:
        json.dump(batch_manifest, f, indent=2)

    print(f"Saved {batch_dir / 'batch_manifest.json'}")


if __name__ == "__main__":
    main()

