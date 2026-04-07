import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
import yaml
import argparse
import time
import platform
import os
import warnings
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score, accuracy_score, classification_report
from scipy.sparse import hstack, csr_matrix
import xgboost as xgb
import ray
from ray.train import RunConfig, ScalingConfig, CheckpointConfig, FailureConfig
from ray.train.xgboost import XGBoostTrainer

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def normalize_merchant(name):
    if not isinstance(name, str):
        return ""
    name = name.upper().strip()
    name = re.sub(r'\b\d{4,}\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def load_and_prepare(config):
    df = pd.read_csv(config['data_path'])
    df = df.rename(columns={
        "Transaction Date": "date",
        "Transaction Type": "transaction_type",
        "Transaction Description": "merchant",
        "Debit Amount": "debit_amount",
        "Credit Amount": "credit_amount",
        "Balance": "balance",
        "Category": "category"
    })
    df.columns = [c.strip().lower() for c in df.columns]
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    df = df.sort_values('date').reset_index(drop=True)
    df['amount'] = df['debit_amount'].fillna(0) - df['credit_amount'].fillna(0)
    df['log_amount'] = np.log1p(df['amount'].abs())
    df['day_of_week'] = df['date'].dt.dayofweek
    df['day_of_month'] = df['date'].dt.day
    df['merchant_clean'] = df['merchant'].apply(normalize_merchant)
    df = df.dropna(subset=['category'])

    split_date = pd.to_datetime(config['split_date'])
    train_df = df[df['date'] < split_date].copy()
    test_df = df[df['date'] >= split_date].copy()

    known_cats = set(train_df['category'].unique())
    test_df = test_df[test_df['category'].isin(known_cats)].copy()

    le = LabelEncoder()
    train_df['label'] = le.fit_transform(train_df['category'])
    test_df['label'] = le.transform(test_df['category'])

    tfidf = TfidfVectorizer(
        analyzer='char_wb',
        ngram_range=(config['tfidf']['ngram_min'], config['tfidf']['ngram_max']),
        max_features=config['tfidf']['max_features']
    )
    X_train_text = tfidf.fit_transform(train_df['merchant_clean'])
    X_test_text = tfidf.transform(test_df['merchant_clean'])

    numeric_cols = ['log_amount', 'day_of_week', 'day_of_month']
    X_train_num = train_df[numeric_cols].fillna(0).values
    X_test_num = test_df[numeric_cols].fillna(0).values

    X_train = hstack([X_train_text, csr_matrix(X_train_num)]).toarray()
    X_test = hstack([X_test_text, csr_matrix(X_test_num)]).toarray()

    feature_cols = [f"f{i}" for i in range(X_train.shape[1])]
    train_pd = pd.DataFrame(X_train, columns=feature_cols)
    train_pd['label'] = train_df['label'].values
    test_pd = pd.DataFrame(X_test, columns=feature_cols)
    test_pd['label'] = test_df['label'].values

    return train_pd, test_pd, le


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m1_ray.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    if 's3' in config:
        os.environ['AWS_ACCESS_KEY_ID'] = config['s3']['access_key']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['s3']['secret_key']
        os.environ['AWS_ENDPOINT_URL'] = config['s3']['endpoint_url']

    mlflow.set_tracking_uri(config['mlflow_tracking_uri'])
    mlflow.set_experiment(config['experiment_name'])

    ray.init(ignore_reinit_error=True, logging_level="ERROR")

    train_pd, test_pd, le = load_and_prepare(config)
    num_classes = len(le.classes_)

    train_ds = ray.data.from_pandas(train_pd)
    valid_ds = ray.data.from_pandas(test_pd)

    params = {
        "objective": "multi:softmax",
        "num_class": num_classes,
        "max_depth": config['model']['max_depth'],
        "learning_rate": config['model']['learning_rate'],
        "eval_metric": "mlogloss",
        "tree_method": "hist",
    }

    trainer = XGBoostTrainer(
        label_column="label",
        params=params,
        num_boost_round=config['model']['n_estimators'],
        scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
        datasets={"train": train_ds, "valid": valid_ds},
        run_config=RunConfig(
            name="m1_ray_xgb",
            storage_path=config['checkpoint_dir'],
            checkpoint_config=CheckpointConfig(
                num_to_keep=2,
                checkpoint_frequency=50,
                checkpoint_at_end=True,
            ),
            failure_config=FailureConfig(max_failures=2),
        ),
    )

    with mlflow.start_run():
        mlflow.log_params({
            "model_type": "xgboost_ray",
            "n_estimators": config['model']['n_estimators'],
            "max_depth": config['model']['max_depth'],
            "learning_rate": config['model']['learning_rate'],
            "tfidf_ngram_min": config['tfidf']['ngram_min'],
            "tfidf_ngram_max": config['tfidf']['ngram_max'],
            "tfidf_max_features": config['tfidf']['max_features'],
            "checkpoint_storage": config['checkpoint_dir'],
            "fault_tolerance": "FailureConfig(max_failures=2)",
            "ray_version": ray.__version__,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        })

        start = time.time()
        result = trainer.fit()
        train_time = time.time() - start

        with result.checkpoint.as_directory() as ckpt_dir:
            # Ray's XGBoostTrainer saves as model.ubj in 2.35
            model_path = os.path.join(ckpt_dir, "model.ubj")
            if not os.path.exists(model_path):
                # fallback for older filename
                model_path = os.path.join(ckpt_dir, "model.json")
            bst = xgb.Booster()
            bst.load_model(model_path)

            X_test_df = test_pd.drop("label", axis=1)
            y_test = test_pd['label'].values
            dtest = xgb.DMatrix(X_test_df.values, feature_names=list(X_test_df.columns))
            y_pred = bst.predict(dtest).astype(int)

            macro_f1 = f1_score(y_test, y_pred, average="macro", zero_division=0)
            weighted_f1 = f1_score(y_test, y_pred, average="weighted", zero_division=0)
            accuracy = accuracy_score(y_test, y_pred)

            mlflow.log_metrics({
                "macro_f1": macro_f1,
                "weighted_f1": weighted_f1,
                "accuracy": accuracy,
                "train_time_seconds": train_time,
                "train_size": len(train_pd),
                "test_size": len(test_pd),
                "num_classes": num_classes,
            })

            present = np.unique(np.concatenate([y_test, y_pred]))
            report = classification_report(
                y_test, y_pred,
                labels=present,
                target_names=le.inverse_transform(present),
                output_dict=True, zero_division=0,
            )
            for cat, metrics in report.items():
                if isinstance(metrics, dict):
                    safe = cat.replace(' ', '_').replace('/', '_')
                    mlflow.log_metric(f"f1_{safe}", metrics['f1-score'])

            mlflow.xgboost.log_model(bst, "model", registered_model_name="m1_categorization")
            print(f"macro_f1={macro_f1:.4f}, accuracy={accuracy:.4f}, weighted_f1={weighted_f1:.4f}, train_time={train_time:.1f}s")

    ray.shutdown()


if __name__ == "__main__":
    main()
