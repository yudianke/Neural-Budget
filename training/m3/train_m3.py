import pandas as pd
import numpy as np
import mlflow
import yaml
import argparse
import time
import platform
from prophet import Prophet
import warnings
import os
import logging

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')
logging.getLogger('prophet').setLevel(logging.ERROR)
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def load_data(path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    return df


def get_monthly_spend(user_df):
    user_df = user_df.copy()
    user_df['month'] = user_df['date'].dt.to_period('M').dt.to_timestamp()
    monthly = user_df.groupby(['month', 'project_category'])['abs_amount'].sum().reset_index()
    monthly.columns = ['month', 'category', 'amount']
    return monthly


def train_user_prophet(monthly_cat_df, config):
    if len(monthly_cat_df) < config['min_months']:
        return None, None

    train = monthly_cat_df.iloc[:-config['split_months']]
    test = monthly_cat_df.iloc[-config['split_months']:]

    df_prophet = train.rename(columns={'month': 'ds', 'amount': 'y'})

    model = Prophet(
        changepoint_prior_scale=config['prophet']['changepoint_prior_scale'],
        seasonality_prior_scale=config['prophet']['seasonality_prior_scale'],
        seasonality_mode=config['prophet']['seasonality_mode'],
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    model.fit(df_prophet)

    future = model.make_future_dataframe(periods=config['split_months'], freq='MS')
    forecast = model.predict(future)
    forecast_test = forecast.tail(config['split_months'])

    mae = np.mean(np.abs(forecast_test['yhat'].values - test['amount'].values))
    return model, mae


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m3.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    mlflow.set_tracking_uri(config['mlflow_tracking_uri'])
    mlflow.set_experiment(config['experiment_name'])

    df = load_data(config['data_path'])
    users = df['synthetic_user_id'].unique()

    trained_models = 0
    total_mae = 0
    mae_count = 0
    start_total = time.time()

    with mlflow.start_run():
        mlflow.log_params({
            "changepoint_prior_scale": config['prophet']['changepoint_prior_scale'],
            "seasonality_prior_scale": config['prophet']['seasonality_prior_scale'],
            "seasonality_mode": config['prophet']['seasonality_mode'],
            "min_months": config['min_months'],
            "split_months": config['split_months'],
            "total_users": len(users),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        })

        for user_id in users[:500]:  # limit to 500 users for initial training
            user_df = df[df['synthetic_user_id'] == user_id]
            if len(user_df) < config['min_transactions']:
                continue

            monthly = get_monthly_spend(user_df)
            categories = monthly['category'].unique()

            for cat in categories:
                cat_df = monthly[monthly['category'] == cat].sort_values('month')
                model, mae = train_user_prophet(cat_df, config)
                if model is not None:
                    trained_models += 1
                    if mae is not None:
                        total_mae += mae
                        mae_count += 1

        train_time = time.time() - start_total
        avg_mae = total_mae / mae_count if mae_count > 0 else 0

        mlflow.log_metrics({
            "trained_models": trained_models,
            "avg_mae": avg_mae,
            "train_time_seconds": train_time,
        })

        print(f"Trained {trained_models} Prophet models | avg_mae={avg_mae:.2f} | train_time={train_time:.1f}s")


if __name__ == "__main__":
    main()
