import pandas as pd
import numpy as np
import mlflow
import yaml
import argparse
import time
import platform
import os
import warnings
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import f1_score
from scipy.sparse import hstack, csr_matrix
import xgboost as xgb
import ray
from ray.train import RunConfig, ScalingConfig, CheckpointConfig, FailureConfig, Checkpoint
from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer as RayXGBoostTrainer
import re
import tempfile

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
    train_df = df[df['date'] < split_date]
    test_df = df[df['date'] >= split_date]

    known_cats = set(train_df['category'].unique())
    test_df = test_df[test_df['category'].isin(known_cats)]

    le = LabelEncoder()
    train_df = train_df.copy()
    test_df = test_df.copy()
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


def train_loop(config):
    train_shard = ray.train.get_dataset_shard("train")
    valid_shard = ray.train.get_dataset_shard("valid")

    train_df = pd.concat([
        pd.DataFrame(batch)
        for batch in train_shard.iter_batches(batch_format="pandas")
    ])
    test_df = pd.concat([
        pd.DataFrame(batch)
        for batch in valid_shard.iter_batches(batch_format="pandas")
    ])

    y_train = train_df.pop('label').values
    y_test = test_df.pop('label').values

    dtrain = xgb.DMatrix(train_df.values, label=y_train)
    dtest = xgb.DMatrix(test_df.values, label=y_test)

    params = config['params']
    num_boost_round = config['num_boost_round']

    # Ray manages checkpoint passing automatically with FailureConfig
    xgb_model = None
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            ckpt_path = os.path.join(ckpt_dir, "model.ubj")
            if os.path.exists(ckpt_path):
                xgb_model = xgb.Booster()
                xgb_model.load_model(ckpt_path)
                print("Resumed XGBoost model from Ray checkpoint")

    callbacks = [RayTrainReportCallback(metrics=["valid-mlogloss"], frequency=50)]

    bst = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtest, "valid")],
        xgb_model=xgb_model,
        callbacks=callbacks,
        verbose_eval=50,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "model.ubj")
        bst.save_model(model_path)
        checkpoint = Checkpoint.from_directory(tmpdir)
        ray.train.report(
            {"valid-mlogloss": float(bst.eval(dtest).split(":")[1])},
            checkpoint=checkpoint
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m1_ray.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    # Set S3 credentials for object storage
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
    test_ds = ray.data.from_pandas(test_pd)

    trainer = RayXGBoostTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config={
            "params": {
                "objective": "multi:softmax",
                "num_class": num_classes,
                "max_depth": config['model']['max_depth'],
                "learning_rate": config['model']['learning_rate'],
                "eval_metric": "mlogloss",
            },
            "num_boost_round": config['model']['n_estimators'],
        },
        scaling_config=ScalingConfig(num_workers=1, use_gpu=False),
        datasets={"train": train_ds, "valid": test_ds},
        run_config=RunConfig(
            storage_path=config['checkpoint_dir'],
            checkpoint_config=CheckpointConfig(num_to_keep=2),
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
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        })

        start = time.time()
        result = trainer.fit()
        train_time = time.time() - start

        checkpoint = result.checkpoint
        with checkpoint.as_directory() as ckpt_dir:
            model_path = os.path.join(ckpt_dir, "model.ubj")
            bst = xgb.Booster()
            bst.load_model(model_path)

            X_test = test_pd.drop('label', axis=1).values
            y_test = test_pd['label'].values
            dtest = xgb.DMatrix(X_test)
            y_pred = bst.predict(dtest).astype(int)

            from sklearn.metrics import accuracy_score, classification_report
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

            # Per-category F1
            report = classification_report(y_test, y_pred,
                                           labels=np.unique(np.concatenate([y_test, y_pred])),
                                           target_names=le.inverse_transform(np.unique(np.concatenate([y_test, y_pred]))),
                                           output_dict=True, zero_division=0)
            for cat, metrics in report.items():
                if isinstance(metrics, dict):
                    mlflow.log_metric(f"f1_{cat.replace(' ', '_')}", metrics['f1-score'])

            mlflow.sklearn.log_model(bst, "xgboost_model", registered_model_name="m1_categorization")
            import mlflow.xgboost as mlflow_xgboost
            mlflow_xgboost.log_model(bst, "model", registered_model_name="m1_categorization")
            print(f"macro_f1={macro_f1:.4f}, accuracy={accuracy:.4f}, weighted_f1={weighted_f1:.4f}, train_time={train_time:.1f}s")

    ray.shutdown()


if __name__ == "__main__":
    main()
