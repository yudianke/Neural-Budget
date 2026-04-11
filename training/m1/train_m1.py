import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import yaml
import argparse
import time
import platform
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.dummy import DummyClassifier
from sklearn.metrics import f1_score, classification_report
from sklearn.preprocessing import LabelEncoder
from scipy.sparse import hstack, csr_matrix
import xgboost as xgb
import re
import os
import warnings
import logging

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')
logging.getLogger('mlflow').setLevel(logging.ERROR)


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


def load_data(path):
    df = pd.read_csv(path)
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
    return df


def split_data(df, split_date):
    train = df[df['date'] < split_date]
    test = df[df['date'] >= split_date]
    known_cats = set(train['category'].unique())
    test = test[test['category'].isin(known_cats)]
    return train, test


def build_model(config, num_classes):
    tfidf = TfidfVectorizer(
        analyzer='char_wb',
        ngram_range=(config['tfidf']['ngram_min'], config['tfidf']['ngram_max']),
        max_features=config['tfidf']['max_features']
    )
    model_type = config['model']['type']
    if model_type == "baseline_dummy":
        clf = DummyClassifier(strategy="most_frequent")
    elif model_type == "baseline_logreg":
        clf = LogisticRegression(max_iter=2000, C=1.0)
    elif model_type == "xgboost":
        clf = xgb.XGBClassifier(
            n_estimators=config['model']['n_estimators'],
            max_depth=config['model']['max_depth'],
            learning_rate=config['model']['learning_rate'],
            eval_metric='mlogloss'
        )
    return tfidf, clf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m1.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    mlflow.set_tracking_uri(config['mlflow_tracking_uri'])
    mlflow.set_experiment(config['experiment_name'])

    df = load_data(config['data_path'])
    train_df, test_df = split_data(df, pd.to_datetime(config['split_date']))

    le = LabelEncoder()
    y_train = le.fit_transform(train_df['category'])
    y_test = le.transform(test_df['category'])
    num_classes = len(le.classes_)

    tfidf, clf = build_model(config, num_classes)

    X_train_text = tfidf.fit_transform(train_df['merchant_clean'])
    X_test_text = tfidf.transform(test_df['merchant_clean'])

    numeric_cols = ['log_amount', 'day_of_week', 'day_of_month']
    X_train_num = train_df[numeric_cols].fillna(0).values
    X_test_num = test_df[numeric_cols].fillna(0).values

    X_train = hstack([X_train_text, csr_matrix(X_train_num)])
    X_test = hstack([X_test_text, csr_matrix(X_test_num)])

    with mlflow.start_run():
        mlflow.log_params({
            "model_type": config['model']['type'],
            "tfidf_ngram_min": config['tfidf']['ngram_min'],
            "tfidf_ngram_max": config['tfidf']['ngram_max'],
            "tfidf_max_features": config['tfidf']['max_features'],
            "split_date": config['split_date'],
            "train_size": len(train_df),
            "test_size": len(test_df),
        })
        if config['model']['type'] == 'xgboost':
            mlflow.log_params({
                "n_estimators": config['model']['n_estimators'],
                "max_depth": config['model']['max_depth'],
                "learning_rate": config['model']['learning_rate'],
            })

        mlflow.log_param("python_version", platform.python_version())
        mlflow.log_param("platform", platform.platform())

        start = time.time()
        clf.fit(X_train, y_train)
        train_time = time.time() - start
        mlflow.log_metric("train_time_seconds", train_time)

        y_pred = clf.predict(X_test)
        macro_f1 = f1_score(y_test, y_pred, average='macro', zero_division=0)
        weighted_f1 = f1_score(y_test, y_pred, average='weighted', zero_division=0)

        mlflow.log_metric("macro_f1", macro_f1)
        mlflow.log_metric("weighted_f1", weighted_f1)

        present_labels = np.unique(np.concatenate([y_test, y_pred]))
        present_names = le.inverse_transform(present_labels)
        report = classification_report(y_test, y_pred,
                                       labels=present_labels,
                                       target_names=present_names,
                                       output_dict=True,
                                       zero_division=0)
        for cat, metrics in report.items():
            if isinstance(metrics, dict):
                mlflow.log_metric(f"f1_{cat.replace(' ', '_')}", metrics['f1-score'])

        mlflow.sklearn.log_model(clf, "model", registered_model_name="m1_categorization")
        print(f"macro_f1={macro_f1:.4f}, weighted_f1={weighted_f1:.4f}, train_time={train_time:.1f}s")


if __name__ == "__main__":
    main()
