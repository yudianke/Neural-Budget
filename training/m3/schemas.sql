CREATE TABLE IF NOT EXISTS monthly_category_history (
    id BIGSERIAL PRIMARY KEY,
    budget_id TEXT NOT NULL,
    category_id TEXT NOT NULL,
    category_name TEXT NOT NULL,
    year_month TEXT NOT NULL,
    monthly_spend NUMERIC NOT NULL,
    budgeted NUMERIC NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (budget_id, category_id, year_month)
);

CREATE TABLE IF NOT EXISTS forecast_training_rows (
    id BIGSERIAL PRIMARY KEY,
    budget_id TEXT NOT NULL,
    category_id TEXT NOT NULL,
    category_name TEXT NOT NULL,
    year_month TEXT NOT NULL,
    monthly_spend NUMERIC NOT NULL,
    lag_1 NUMERIC NOT NULL,
    lag_2 NUMERIC NOT NULL,
    lag_3 NUMERIC NOT NULL,
    lag_6 NUMERIC NOT NULL,
    rolling_mean_3 NUMERIC NOT NULL,
    rolling_std_3 NUMERIC NOT NULL,
    rolling_mean_6 NUMERIC NOT NULL,
    rolling_max_3 NUMERIC NOT NULL,
    history_month_count INTEGER NOT NULL,
    month_num INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_q4 INTEGER NOT NULL,
    month_sin NUMERIC NOT NULL,
    month_cos NUMERIC NOT NULL,
    budgeted NUMERIC,
    target_next_month NUMERIC,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_versions (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    overall_mae NUMERIC,
    median_per_category_mae NUMERIC,
    trained_at TIMESTAMP NOT NULL DEFAULT NOW(),
    promoted_at TIMESTAMP,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS retraining_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    train_rows INTEGER,
    eval_rows INTEGER,
    candidate_version TEXT,
    promoted BOOLEAN DEFAULT FALSE,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS local_user_transactions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    date TEXT NOT NULL,
    category_id TEXT,
    category_name TEXT,
    amount NUMERIC NOT NULL,
    payee TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS local_monthly_category_history (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    category_id TEXT,
    category_name TEXT,
    year_month TEXT NOT NULL,
    monthly_spend NUMERIC NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, category_id, year_month)
);