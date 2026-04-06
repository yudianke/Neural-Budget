import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import json
from pathlib import Path


"""
combined the three datasets by using each for a different role in the synthetic data generation pipeline:
fmli used to generate synthetic users (demographics and persona clusters)
mtbi  used to learn spending distributions across categories
moneydata  used to model transaction-level behavior (merchants, amounts, timing)
"""


def load_config(config_path: str = "data_pipeline/manifest.json") -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def map_ucc_to_category(ucc) -> str:
    try:
        ucc = int(ucc)
    except Exception:
        return "other"

    if 190000 <= ucc < 200000:
        return "housing"
    if 200000 <= ucc < 230000:
        return "groceries"
    if 230000 <= ucc < 250000:
        return "restaurants"
    if 250000 <= ucc < 270000:
        return "utilities"
    if 270000 <= ucc < 300000:
        return "shopping"
    if 300000 <= ucc < 340000:
        return "transport"
    if 340000 <= ucc < 350000:
        return "gas"
    if 370000 <= ucc < 400000:
        return "insurance"
    if 400000 <= ucc < 440000:
        return "healthcare"
    if 500000 <= ucc < 600000:
        return "entertainment"
    if 600000 <= ucc < 610000:
        return "personal_care"
    if 610000 <= ucc < 650000:
        return "shopping"
    if 650000 <= ucc < 670000:
        return "education"
    if 680000 <= ucc < 700000:
        return "charity"
    if 700000 <= ucc < 800000:
        return "taxes"
    if 800000 <= ucc < 900000:
        return "cash_transfers"
    return "misc"


MANUAL_MERCHANT_MAP = {
    "LIDL GB  NOTTINGHA": "groceries",
    "LIDL GB NOTTINGHAM": "groceries",
    "LNK TESCO FABIAN W": "groceries",
    "ORIENTAL MART HYDR": "groceries",
    "MARKS&SPENCER PLC": "groceries",
    "LNK M&S SWANSEA": "groceries",
    "AMAZON UK MARKETPL": "shopping",
    "AMZNMKTPLACE": "shopping",
    "AMAZON UK RETAIL": "shopping",
    "AMZNMKTPLACE AMAZO": "shopping",
    "AMAZON SVCS EU-UK": "shopping",
    "AMAZON UK RETAIL A": "shopping",
    "AMAZON SVCS EUROPE": "shopping",
    "LNK UNI - UNIVERSI": "education",
    "LOYD WALES UNIV SW": "education",
    "SWANSEA UNIVERSITY": "education",
    "SWANSEA UNI - PAY": "education",
    "UNIV OF NOTTINGHAM": "education",
    "VIRGIN MEDIA PYMTS": "utilities",
    "DWR CYMRU W WATER": "utilities",
    "GOOD ENERGY LTD": "utilities",
    "TALKTALK LIMITED": "utilities",
    "O2": "utilities",
    "CC SWANSEA C.TAX": "utilities",
    "UBER   *TRIP": "transport",
    "ARRIVA TRAINS WALE": "transport",
    "ASSURANT INTER LTD": "insurance",
    "LV LIFE": "insurance",
    "ZURICH": "insurance",
    "TAMBA TAM": "restaurants",
    "SUBWAY 32610 MIDLA": "restaurants",
    "BRONTOSAURUS VEGAN": "restaurants",
    "TRADING212UK": "misc",
    "WWW.III.CO.UK DE": "misc",
    "NON-GBP TRANS FEE": "misc",
    "NON-GBP PURCH FEE": "misc",
    "NON-STG TRANS FEE": "misc",
    "NON-STG PURCH FEE": "misc",
    "ACCOUNT FEE": "misc",
    "CLUB LLOYDS FEE": "misc",
    "CLUB LLOYDS WAIVED": "misc",
    "INTEREST (GROSS)": "misc",
    "ESAVINGS ACCOUNT": "misc",
    "SAVE THE CHANGE": "misc",
    "UCU B ACCOUNT": "misc",
    "ATRIUM JUBILEE 1": "misc",
    "ANTIGONI ABRAHMS": "misc",
    "MISS D WU": "misc",
    "ALICJA ALEXANDER": "misc",
}


def map_merchant_keyword_fallback(merchant: str) -> str:
    m = merchant.lower()

    if any(x in m for x in ["tesco", "lidl", "sainsbury", "aldi", "marks&spencer", "m&s", "oriental mart", "coop", "co-op"]):
        return "groceries"

    if any(x in m for x in ["subway", "pizza", "cafe", "coffee", "restaurant", "kfc", "mcdonald", "burger", "vegan", "tamba"]):
        return "restaurants"

    if any(x in m for x in ["amazon", "marketpl", "retail", "shop", "store"]):
        return "shopping"

    if any(x in m for x in ["media", "water", "energy", "electric", "talktalk", "broadband", "mobile", "phone", "o2", "c.tax", "council tax"]):
        return "utilities"

    if any(x in m for x in ["uber", "train", "rail", "bus", "arriva", "travel", "transport"]):
        return "transport"

    if any(x in m for x in ["shell", "petrol", "fuel", "esso", "bp "]):
        return "gas"

    if any(x in m for x in ["assurant", "lv life", "zurich", "insurance"]):
        return "insurance"

    if any(x in m for x in ["university", "uni ", "college", "nottingham", "swansea"]):
        return "education"

    if any(x in m for x in ["spotify", "netflix", "audible", "prime", "cinema", "theatre"]):
        return "entertainment"

    if any(x in m for x in ["pharmacy", "dental", "clinic", "hospital", "optician", "medical"]):
        return "healthcare"

    return "misc"


def assign_moneydata_category(merchant: str) -> str:
    if merchant in MANUAL_MERCHANT_MAP:
        return MANUAL_MERCHANT_MAP[merchant]
    return map_merchant_keyword_fallback(merchant)


def random_dates_in_month(year: int, month: int, n: int, rng: np.random.Generator):
    start = pd.Timestamp(year=year, month=month, day=1)
    if month == 12:
        end = pd.Timestamp(year=year + 1, month=1, day=1) - pd.Timedelta(days=1)
    else:
        end = pd.Timestamp(year=year, month=month + 1, day=1) - pd.Timedelta(days=1)

    days = pd.date_range(start, end, freq="D")
    if len(days) == 0 or n <= 0:
        return []

    sampled = rng.choice(days, size=n, replace=True)
    return sorted(pd.to_datetime(sampled))


def choose_transaction_type(category: str, rng: np.random.Generator) -> str:
    if category in {"housing", "utilities", "insurance"}:
        return rng.choice(["DD", "BP", "SO"], p=[0.5, 0.3, 0.2])
    return rng.choice(["DEB", "BP"], p=[0.85, 0.15])


def generate_amounts(total_amount: float, n_txns: int, noise_ratio: float, rng: np.random.Generator):
    if n_txns <= 0:
        return []

    base = total_amount / n_txns
    vals = []
    for _ in range(n_txns):
        noisy = rng.normal(loc=base, scale=max(base * noise_ratio, 1.0))
        vals.append(max(noisy, 1.0))

    vals = np.array(vals)
    vals = vals / vals.sum() * total_amount
    return np.round(vals, 2).tolist()


def main() -> None:
    config = load_config()
    rng = np.random.default_rng(config["random_seed"])

    mtbi_path = Path(config["mtbi_path"])
    fmli_path = Path(config["fmli_path"])
    moneydata_path = Path(config["moneydata_path"])
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    # CES
    mtbi = pd.read_csv(mtbi_path)
    fmli = pd.read_csv(fmli_path)

    mtbi.columns = mtbi.columns.str.upper()
    fmli.columns = fmli.columns.str.upper()

    mtbi = mtbi[["NEWID", "UCC", "COST"]].copy()
    mtbi["COST"] = pd.to_numeric(mtbi["COST"], errors="coerce")
    mtbi = mtbi.dropna(subset=["NEWID", "UCC", "COST"]).copy()

    fmli_keep = ["NEWID", "AGE_REF", "SEX_REF", "FAM_SIZE", "FINLWT21"]
    fmli_keep = [c for c in fmli_keep if c in fmli.columns]
    fmli = fmli[fmli_keep].copy()

    mtbi["category"] = mtbi["UCC"].apply(map_ucc_to_category)

    ces_long = (
        mtbi.groupby(["NEWID", "category"])["COST"]
        .sum()
        .reset_index()
        .rename(columns={"NEWID": "household_id", "COST": "annual_spend"})
    )
    ces_long = ces_long[ces_long["annual_spend"] > 10].copy()

    ces_wide = (
        ces_long.pivot(index="household_id", columns="category", values="annual_spend")
        .fillna(0)
        .reset_index()
    )

    if "NEWID" in fmli.columns:
        fmli = fmli.rename(columns={"NEWID": "household_id"})
    ces_wide = ces_wide.merge(fmli, on="household_id", how="left")

    sample_weights = None
    if "FINLWT21" in ces_wide.columns:
        weights = pd.to_numeric(ces_wide["FINLWT21"], errors="coerce").fillna(0)
        if weights.sum() > 0:
            sample_weights = weights / weights.sum()

    sampled_households = rng.choice(
        ces_wide["household_id"].values,
        size=config["n_synthetic_users"],
        replace=True,
        p=sample_weights if sample_weights is not None else None,
    )

    synthetic = ces_wide.set_index("household_id").loc[sampled_households].reset_index()
    synthetic = synthetic.rename(columns={"household_id": "source_household_id"})
    synthetic.insert(
        0,
        "synthetic_user_id",
        [f"user_{i+1}" for i in range(config["n_synthetic_users"])],
    )
    synthetic["user_scale"] = rng.uniform(0.6, 1.6, size=len(synthetic))

    exclude_cols = {
        "synthetic_user_id",
        "source_household_id",
        "AGE_REF",
        "SEX_REF",
        "FAM_SIZE",
        "FINLWT21",
        "user_scale",
    }
    category_cols = [c for c in synthetic.columns if c not in exclude_cols]

    for col in category_cols:
        synthetic[col] = synthetic[col] * synthetic["user_scale"]

    for col in config["drop_categories"]:
        if col in synthetic.columns:
            synthetic = synthetic.drop(columns=[col])

    exclude_cols = exclude_cols.union(set(config["drop_categories"]))
    category_cols = [c for c in synthetic.columns if c not in exclude_cols]

    for col in config["important_cols_for_smoothing"]:
        if col in synthetic.columns:
            mask = synthetic[col] == 0
            synthetic.loc[mask, col] = rng.uniform(200, 1000, size=mask.sum())

    if "misc" in synthetic.columns:
        if "shopping" in synthetic.columns:
            synthetic["shopping"] += synthetic["misc"] * 0.2
        if "entertainment" in synthetic.columns:
            synthetic["entertainment"] += synthetic["misc"] * 0.1
        synthetic["misc"] = synthetic["misc"] * 0.7
        synthetic["misc"] = synthetic["misc"].clip(upper=10000)

    cluster_features = [c for c in synthetic.columns if c not in exclude_cols]
    kmeans = KMeans(
        n_clusters=config["persona_clusters"],
        random_state=config["random_seed"],
        n_init=10,
    )
    synthetic["persona_cluster"] = kmeans.fit_predict(synthetic[cluster_features])

    # MoneyData
    money_df = pd.read_csv(moneydata_path)

    money_df = money_df.rename(columns={
        "Transaction Date": "date",
        "Transaction Type": "transaction_type",
        "Transaction Description": "merchant",
        "Debit Amount": "debit_amount",
        "Credit Amount": "credit_amount",
        "Balance": "balance",
    })
    money_df.columns = [c.strip().lower() for c in money_df.columns]

    for col in ["debit_amount", "credit_amount", "balance"]:
        if col in money_df.columns:
            money_df[col] = pd.to_numeric(money_df[col], errors="coerce")

    money_df["date"] = pd.to_datetime(money_df["date"], errors="coerce", dayfirst=True)
    money_df["debit_amount"] = money_df["debit_amount"].fillna(0)
    money_df["credit_amount"] = money_df["credit_amount"].fillna(0)
    money_df["amount"] = money_df["credit_amount"] - money_df["debit_amount"]

    money_df["merchant"] = (
        money_df["merchant"]
        .fillna("UNKNOWN")
        .astype(str)
        .str.upper()
        .str.strip()
    )
    money_df["transaction_type"] = (
        money_df["transaction_type"]
        .fillna("UNKNOWN")
        .astype(str)
        .str.upper()
        .str.strip()
    )
    money_df = money_df.dropna(subset=["date", "merchant"]).reset_index(drop=True)

    money_df["project_category"] = money_df["merchant"].apply(assign_moneydata_category)

    merchant_pool = (
        money_df.groupby("project_category")["merchant"]
        .apply(lambda s: list(s.value_counts().index))
        .to_dict()
    )

    # Synthetic transactions
    frequency_rules = {
        "housing": {"monthly_txn_range": (1, 1), "amount_noise": 0.03},
        "utilities": {"monthly_txn_range": (1, 3), "amount_noise": 0.08},
        "groceries": {"monthly_txn_range": (6, 18), "amount_noise": 0.20},
        "restaurants": {"monthly_txn_range": (2, 10), "amount_noise": 0.25},
        "transport": {"monthly_txn_range": (4, 20), "amount_noise": 0.25},
        "gas": {"monthly_txn_range": (1, 6), "amount_noise": 0.18},
        "insurance": {"monthly_txn_range": (0, 2), "amount_noise": 0.05},
        "healthcare": {"monthly_txn_range": (0, 4), "amount_noise": 0.25},
        "entertainment": {"monthly_txn_range": (0, 6), "amount_noise": 0.30},
        "shopping": {"monthly_txn_range": (1, 10), "amount_noise": 0.35},
        "education": {"monthly_txn_range": (0, 3), "amount_noise": 0.18},
        "charity": {"monthly_txn_range": (0, 2), "amount_noise": 0.15},
        "personal_care": {"monthly_txn_range": (0, 3), "amount_noise": 0.20},
        "misc": {"monthly_txn_range": (0, 5), "amount_noise": 0.35},
    }

    def choose_merchant(category: str) -> str:
        choices = merchant_pool.get(category, [])
        if len(choices) == 0:
            return f"{category.upper()}_MERCHANT"
        return rng.choice(choices[: min(15, len(choices))])

    months = pd.period_range(
        start=config["start_month"],
        end=config["end_month"],
        freq="M",
    )
    all_transactions = []

    for _, row in synthetic.iterrows():
        user_id = row["synthetic_user_id"]
        source_household_id = row["source_household_id"]
        persona_cluster = row["persona_cluster"]

        for category in category_cols:
            annual_spend = float(row.get(category, 0))
            if annual_spend <= 0:
                continue

            monthly_budget = annual_spend / 12.0
            freq_rule = frequency_rules.get(category, {"monthly_txn_range": (1, 4), "amount_noise": 0.25})

            for period in months:
                low, high = freq_rule["monthly_txn_range"]
                n_txns = int(rng.integers(low, high + 1))
                if n_txns == 0:
                    continue

                txn_dates = random_dates_in_month(period.year, period.month, n_txns, rng)
                txn_amounts = generate_amounts(monthly_budget, n_txns, freq_rule["amount_noise"], rng)

                for txn_date, amt in zip(txn_dates, txn_amounts):
                    all_transactions.append({
                        "synthetic_user_id": user_id,
                        "source_household_id": source_household_id,
                        "persona_cluster": int(persona_cluster),
                        "date": txn_date,
                        "merchant": choose_merchant(category),
                        "project_category": category,
                        "transaction_type": choose_transaction_type(category, rng),
                        "amount": -abs(float(amt)),
                        "is_synthetic": 1,
                    })

    synthetic_txns = pd.DataFrame(all_transactions)
    synthetic_txns = synthetic_txns.sort_values(
        ["synthetic_user_id", "date", "project_category"]
    ).reset_index(drop=True)

    synthetic_txns["transaction_id"] = [
        f"txn_{i+1:09d}" for i in range(len(synthetic_txns))
    ]
    synthetic_txns["abs_amount"] = synthetic_txns["amount"].abs()
    synthetic_txns["day_of_week"] = pd.to_datetime(synthetic_txns["date"]).dt.dayofweek
    synthetic_txns["day_of_month"] = pd.to_datetime(synthetic_txns["date"]).dt.day
    synthetic_txns["month"] = pd.to_datetime(synthetic_txns["date"]).dt.month
    synthetic_txns["log_abs_amount"] = np.log1p(synthetic_txns["abs_amount"])
    synthetic_txns["abs_amount_rounded"] = synthetic_txns["abs_amount"].round(2)

    repeat_counts = (
        synthetic_txns.groupby(["synthetic_user_id", "merchant", "abs_amount_rounded"])
        .size()
        .reset_index(name="repeat_count")
    )

    synthetic_txns = synthetic_txns.merge(
        repeat_counts,
        on=["synthetic_user_id", "merchant", "abs_amount_rounded"],
        how="left",
    )
    synthetic_txns["is_recurring_candidate"] = (
        synthetic_txns["repeat_count"] >= 3
    ).astype(int)

    # Save outputs
    users_output = output_dir / "synthetic_users.csv"
    txns_output = output_dir / "synthetic_transactions.csv"
    money_output = output_dir / "moneydata_labeled.csv"
    ces_output = output_dir / "ces_household_category_spend.csv"
    manifest_output = output_dir / "generation_manifest.json"

    synthetic.to_csv(users_output, index=False)
    synthetic_txns.to_csv(txns_output, index=False)
    money_df.to_csv(money_output, index=False)
    ces_long.to_csv(ces_output, index=False)

    manifest = {
        "n_synthetic_users": int(config["n_synthetic_users"]),
        "synthetic_user_count": int(synthetic["synthetic_user_id"].nunique()),
        "synthetic_transaction_count": int(len(synthetic_txns)),
        "persona_clusters": sorted(synthetic["persona_cluster"].unique().tolist()),
        "categories_used": sorted(category_cols),
        "random_seed": int(config["random_seed"]),
        "merchant_pool_categories": sorted(list(merchant_pool.keys())),
    }
    with open(manifest_output, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Saved {users_output}")
    print(f"Saved {txns_output}")
    print(f"Saved {money_output}")
    print(f"Saved {ces_output}")
    print(f"Saved {manifest_output}")


if __name__ == "__main__":
    main()
