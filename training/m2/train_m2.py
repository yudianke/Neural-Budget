import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import yaml
import argparse
import time
import platform
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
import os

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def load_data(path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['synthetic_user_id', 'date']).reset_index(drop=True)
    return df


def compute_features(user_df):
    user_df = user_df.copy().sort_values('date').reset_index(drop=True)
    user_df['rolling_mean'] = user_df['abs_amount'].rolling(30, min_periods=1).mean()
    user_df['rolling_std'] = user_df['abs_amount'].rolling(30, min_periods=1).std().fillna(0)
    user_df['amount_zscore'] = (user_df['abs_amount'] - user_df['rolling_mean']) / (user_df['rolling_std'] + 1e-6)
    user_df['weekly_count'] = user_df['date'].dt.isocalendar().week
    user_df['freq_ratio'] = user_df.groupby('weekly_count')['transaction_id'].transform('count') / (user_df['weekly_count'].nunique() + 1e-6)
    return user_df


def train_user_model(user_df, config):
    features = ['abs_amount', 'log_abs_amount', 'amount_zscore', 'freq_ratio',
                'repeat_count', 'is_recurring_candidate', 'day_of_week', 'day_of_month']
    X = user_df[features].fillna(0).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    clf = IsolationForest(
        contamination=config['isolation_forest']['contamination'],
        n_estimators=config['isolation_forest']['n_estimators'],
        random_state=config['isolation_forest']['random_state']
    )
    clf.fit(X_scaled)
    return clf, scaler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m2.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    mlflow.set_tracking_uri(config['mlflow_tracking_uri'])
    mlflow.set_experiment(config['experiment_name'])

    df = load_data(config['data_path'])
    users = df['synthetic_user_id'].unique()

    trained_users = 0
    total_anomalies = 0
    start_total = time.time()

    with mlflow.start_run():
        mlflow.log_params({
            "contamination": config['isolation_forest']['contamination'],
            "n_estimators": config['isolation_forest']['n_estimators'],
            "random_state": config['isolation_forest']['random_state'],
            "min_transactions": config['min_transactions'],
            "total_users": len(users),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        })

        total_users = len(users)
        processed = 0
        for user_id in users:
            user_df = df[df['synthetic_user_id'] == user_id]
            if len(user_df) < config['min_transactions']:
                continue

            user_df = compute_features(user_df)
            clf, scaler = train_user_model(user_df, config)

            scores = clf.decision_function(
                scaler.transform(
                    user_df[['abs_amount', 'log_abs_amount', 'amount_zscore',
                              'freq_ratio', 'repeat_count', 'is_recurring_candidate',
                              'day_of_week', 'day_of_month']].fillna(0).values
                )
            )
            preds = clf.predict(
                scaler.transform(
                    user_df[['abs_amount', 'log_abs_amount', 'amount_zscore',
                              'freq_ratio', 'repeat_count', 'is_recurring_candidate',
                              'day_of_week', 'day_of_month']].fillna(0).values
                )
            )
            n_anomalies = (preds == -1).sum()
            total_anomalies += n_anomalies
            trained_users += 1
            processed += 1
            if processed % 100 == 0:
                print(f"Progress: {processed}/{total_users} users | trained={trained_users}", flush=True)

        train_time = time.time() - start_total
        anomaly_rate = total_anomalies / len(df) if len(df) > 0 else 0

        mlflow.log_metrics({
            "trained_users": trained_users,
            "total_transactions": len(df),
            "total_anomalies_flagged": total_anomalies,
            "anomaly_rate": anomaly_rate,
            "train_time_seconds": train_time,
        })

        mlflow.log_param("model_type", "isolation_forest")
        print(f"Trained {trained_users} users | anomaly_rate={anomaly_rate:.4f} | train_time={train_time:.1f}s")


if __name__ == "__main__":
    main()
