import json
from pathlib import Path
from typing import List, Optional
import pandas as pd


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
        "num_users": df["synthetic_user_id"].nunique()
        if "synthetic_user_id" in df.columns
        else None,
        "date_range": range_info,
    }


def load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        config_path = Path(__file__).parent / "manifest.json"
    else:
        config_path = Path(config_path)

    with open(config_path, "r") as f:
        return json.load(f)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    print(f"Saved {path}")


def add_monthly_forecast_features(
    monthly_spend: pd.DataFrame,
    users: pd.DataFrame,
    min_history_months: int = 3,
) -> pd.DataFrame:
    """
    Build supervised next-month forecasting rows for each
    (synthetic_user_id, project_category, year_month).

    Features use ONLY prior months.
    Target is next month's spend for the same user-category pair.
    """
    df = monthly_spend.copy()

    # Robust time column
    df["year_month_dt"] = pd.to_datetime(df["year_month"] + "-01", errors="coerce")
    df = df.dropna(subset=["year_month_dt"]).copy()

    # Build a complete monthly panel so lags represent true monthly history
    all_users = sorted(df["synthetic_user_id"].dropna().unique().tolist())
    all_categories = sorted(df["project_category"].dropna().unique().tolist())
    all_months = pd.date_range(
        start=df["year_month_dt"].min(),
        end=df["year_month_dt"].max(),
        freq="MS",
    )

    full_index = pd.MultiIndex.from_product(
        [all_users, all_categories, all_months],
        names=["synthetic_user_id", "project_category", "year_month_dt"],
    )

    df = (
        df.set_index(["synthetic_user_id", "project_category", "year_month_dt"])
        .reindex(full_index)
        .reset_index()
    )

    df["monthly_spend"] = df["monthly_spend"].fillna(0.0)
    df["year_month"] = df["year_month_dt"].dt.to_period("M").astype(str)

    df = df.sort_values(
        ["synthetic_user_id", "project_category", "year_month_dt"]
    ).reset_index(drop=True)

    group_cols = ["synthetic_user_id", "project_category"]
    grouped = df.groupby(group_cols, group_keys=False)

    # Lag features
    for lag in [1, 2, 3, 6]:
        df[f"lag_{lag}"] = grouped["monthly_spend"].shift(lag)

    # Prior-only rolling features
    prior_spend = grouped["monthly_spend"].shift(1)

    df["rolling_mean_3"] = (
        prior_spend.groupby([df["synthetic_user_id"], df["project_category"]])
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    df["rolling_std_3"] = (
        prior_spend.groupby([df["synthetic_user_id"], df["project_category"]])
        .rolling(window=3, min_periods=2)
        .std()
        .reset_index(level=[0, 1], drop=True)
    )

    df["rolling_mean_6"] = (
        prior_spend.groupby([df["synthetic_user_id"], df["project_category"]])
        .rolling(window=6, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    df["rolling_max_3"] = (
        prior_spend.groupby([df["synthetic_user_id"], df["project_category"]])
        .rolling(window=3, min_periods=1)
        .max()
        .reset_index(level=[0, 1], drop=True)
    )

    # Count of prior observed months for this user-category
    df["history_month_count"] = grouped.cumcount()

    # Time features
    df["month_num"] = df["year_month_dt"].dt.month
    df["quarter"] = df["year_month_dt"].dt.quarter
    df["year"] = df["year_month_dt"].dt.year
    df["is_q4"] = df["month_num"].isin([10, 11, 12]).astype(int)

    # User total spend context
    user_month = (
        df.groupby(["synthetic_user_id", "year_month_dt"], as_index=False)["monthly_spend"]
        .sum()
        .rename(columns={"monthly_spend": "user_total_monthly_spend"})
        .sort_values(["synthetic_user_id", "year_month_dt"])
    )

    user_month["user_total_lag_1"] = (
        user_month.groupby("synthetic_user_id")["user_total_monthly_spend"].shift(1)
    )

    user_month["user_total_rolling_mean_3"] = (
        user_month.groupby("synthetic_user_id")["user_total_monthly_spend"]
        .shift(1)
        .groupby(user_month["synthetic_user_id"])
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df = df.merge(
        user_month[
            [
                "synthetic_user_id",
                "year_month_dt",
                "user_total_lag_1",
                "user_total_rolling_mean_3",
            ]
        ],
        on=["synthetic_user_id", "year_month_dt"],
        how="left",
    )

    df["category_share_lag_1"] = (
        df["lag_1"] / df["user_total_lag_1"].replace(0, pd.NA)
    )

    # Merge stable user features
    user_feature_cols: List[str] = [
        c
        for c in [
            "synthetic_user_id",
            "persona_cluster",
            "AGE_REF",
            "FAM_SIZE",
            "user_scale",
        ]
        if c in users.columns
    ]

    if len(user_feature_cols) > 1:
        user_features = users[user_feature_cols].drop_duplicates("synthetic_user_id")
        df = df.merge(user_features, on="synthetic_user_id", how="left")

    # Target: next month's spend for the same user-category
    df["target_next_month_spend"] = grouped["monthly_spend"].shift(-1)

    # Keep only rows with a valid target
    df = df.dropna(subset=["target_next_month_spend"]).copy()

    # Require minimum history
    df = df[df["history_month_count"] >= min_history_months].copy()

    # Fill edge NaNs for numeric features
    numeric_fill_zero = [
        "lag_1",
        "lag_2",
        "lag_3",
        "lag_6",
        "rolling_mean_3",
        "rolling_std_3",
        "rolling_mean_6",
        "rolling_max_3",
        "user_total_lag_1",
        "user_total_rolling_mean_3",
        "category_share_lag_1",
    ]

    for col in numeric_fill_zero:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)

    df = df.sort_values(
        ["synthetic_user_id", "project_category", "year_month_dt"]
    ).reset_index(drop=True)

    final_cols = [
        "synthetic_user_id",
        "project_category",
        "year_month",
        "monthly_spend",
        "lag_1",
        "lag_2",
        "lag_3",
        "lag_6",
        "rolling_mean_3",
        "rolling_std_3",
        "rolling_mean_6",
        "rolling_max_3",
        "history_month_count",
        "month_num",
        "quarter",
        "year",
        "is_q4",
        "user_total_lag_1",
        "user_total_rolling_mean_3",
        "category_share_lag_1",
        "persona_cluster",
        "AGE_REF",
        "FAM_SIZE",
        "user_scale",
        "target_next_month_spend",
    ]
    final_cols = [c for c in final_cols if c in df.columns]

    return df[final_cols].copy()


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
    categorization_df = categorization_df.sort_values("date").reset_index(drop=True)

    split_idx = int(len(categorization_df) * 0.8)
    categorization_train = categorization_df.iloc[:split_idx].copy()
    categorization_eval = categorization_df.iloc[split_idx:].copy()

    save_csv(categorization_train, batch_dir / "categorization_train.csv")
    save_csv(categorization_eval, batch_dir / "categorization_eval.csv")

    # Anomaly dataset
    anomaly_df = txns.copy()

    user_counts = anomaly_df.groupby("synthetic_user_id").size().reset_index(name="txn_count")
    eligible_users = user_counts[user_counts["txn_count"] >= 20]["synthetic_user_id"]

    anomaly_df = anomaly_df[anomaly_df["synthetic_user_id"].isin(eligible_users)].copy()
    anomaly_df = anomaly_df.sort_values(["synthetic_user_id", "date"]).reset_index(drop=True)

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

    # Forecasting dataset v1 (baseline monthly table)
    forecasting_df = txns.copy()
    forecasting_df["year_month"] = forecasting_df["date"].dt.to_period("M").astype(str)

    monthly_spend = (
        forecasting_df.groupby(
            ["synthetic_user_id", "year_month", "project_category"],
            as_index=False
        )["abs_amount"]
        .sum()
        .rename(columns={"abs_amount": "monthly_spend"})
    )

    monthly_spend = monthly_spend.sort_values(
        ["synthetic_user_id", "year_month", "project_category"]
    ).reset_index(drop=True)

    unique_months = sorted(monthly_spend["year_month"].unique().tolist())
    split_month_idx = max(1, int(len(unique_months) * 0.8))
    train_months = set(unique_months[:split_month_idx])
    eval_months = set(unique_months[split_month_idx:])

    forecasting_train = monthly_spend[
        monthly_spend["year_month"].isin(train_months)
    ].copy()
    forecasting_eval = monthly_spend[
        monthly_spend["year_month"].isin(eval_months)
    ].copy()

    save_csv(forecasting_train, batch_dir / "forecasting_train.csv")
    save_csv(forecasting_eval, batch_dir / "forecasting_eval.csv")

    # Forecasting dataset v2 (personalized, time-aware supervised rows)
    forecasting_v2_df = add_monthly_forecast_features(
        monthly_spend=monthly_spend,
        users=users,
        min_history_months=3,
    )

    v2_unique_months = sorted(forecasting_v2_df["year_month"].unique().tolist())
    v2_split_month_idx = max(1, int(len(v2_unique_months) * 0.8))
    v2_train_months = set(v2_unique_months[:v2_split_month_idx])
    v2_eval_months = set(v2_unique_months[v2_split_month_idx:])

    forecasting_v2_train = forecasting_v2_df[
        forecasting_v2_df["year_month"].isin(v2_train_months)
    ].copy()
    forecasting_v2_eval = forecasting_v2_df[
        forecasting_v2_df["year_month"].isin(v2_eval_months)
    ].copy()

    save_csv(forecasting_v2_train, batch_dir / "forecasting_v2_train.csv")
    save_csv(forecasting_v2_eval, batch_dir / "forecasting_v2_eval.csv")

    # Save batch manifest
    batch_manifest = {
        "source_files": {
            "synthetic_transactions": str(txns_path),
            "synthetic_users": str(users_path),
        },
        "selection_rules": {
            "categorization": "All transactions with required fields; chronological 80/20 split.",
            "anomaly": "Only users with >= 20 transactions; rolling history features computed from prior transactions only; chronological 80/20 split.",
            "forecasting": "Monthly aggregation by user and category; split by month to avoid future leakage.",
            "forecasting_v2": "Supervised next-month user-category forecasting rows with lag and rolling features computed from prior months only; chronological month split.",
        },
        "leakage_prevention": [
            "No random split for time-dependent data.",
            "Chronological split used for categorization and anomaly datasets.",
            "Forecasting split uses earlier months for training and later months for evaluation.",
            "Forecasting_v2 lag and rolling features are computed using prior months only.",
            "Anomaly historical features are computed using prior transactions only.",
        ],
        "dataset_summaries": [
            dataset_summary(categorization_train, "categorization_train"),
            dataset_summary(categorization_eval, "categorization_eval"),
            dataset_summary(anomaly_train, "anomaly_train"),
            dataset_summary(anomaly_eval, "anomaly_eval"),
            dataset_summary(forecasting_train, "forecasting_train"),
            dataset_summary(forecasting_eval, "forecasting_eval"),
            dataset_summary(forecasting_v2_train, "forecasting_v2_train"),
            dataset_summary(forecasting_v2_eval, "forecasting_v2_eval"),
        ],
        "outputs": [
            "categorization_train.csv",
            "categorization_eval.csv",
            "anomaly_train.csv",
            "anomaly_eval.csv",
            "forecasting_train.csv",
            "forecasting_eval.csv",
            "forecasting_v2_train.csv",
            "forecasting_v2_eval.csv",
        ],
    }

    with open(batch_dir / "batch_manifest.json", "w") as f:
        json.dump(batch_manifest, f, indent=2)

    print(f"Saved {batch_dir / 'batch_manifest.json'}")


if __name__ == "__main__":
    main()