"""Fault-tolerance demo: run M1 training with a deliberate crash.

The FailOnceCallback raises at a chosen boost round on the first attempt.
Ray's FailureConfig(max_failures=2) retries the trial, which loads the
last checkpoint from S3 and resumes instead of restarting from round 0.
"""
import os
import time
import platform
import argparse
import warnings
import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
import xgboost as xgb
import ray
from ray.train import RunConfig, ScalingConfig, CheckpointConfig, FailureConfig
from ray.train.xgboost import XGBoostTrainer
from sklearn.metrics import f1_score, accuracy_score

from train_m1_ray import load_config, load_and_prepare

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')

SENTINEL = "/tmp/ft_demo_sentinel/failed_once"


class FailOnceCallback(xgb.callback.TrainingCallback):
    def __init__(self, fail_at_round):
        self.fail_at_round = fail_at_round

    def after_iteration(self, model, epoch, evals_log):
        if epoch == self.fail_at_round and not os.path.exists(SENTINEL):
            os.makedirs(os.path.dirname(SENTINEL), exist_ok=True)
            with open(SENTINEL, "w") as f:
                f.write(f"crashed at round {epoch}\n")
            raise RuntimeError(
                f"[FaultInject] Simulated crash at boost round {epoch}"
            )
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m1_ray.yaml')
    parser.add_argument('--fail-at', type=int, default=75,
                        help='Boost round at which to inject failure')
    args = parser.parse_args()
    config = load_config(args.config)

    # Clear sentinel at start so each run demonstrates fresh
    if os.path.exists(SENTINEL):
        os.remove(SENTINEL)

    if 's3' in config:
        os.environ['AWS_ACCESS_KEY_ID'] = config['s3']['access_key']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['s3']['secret_key']
        os.environ['AWS_ENDPOINT_URL'] = config['s3']['endpoint_url']

    mlflow.set_tracking_uri(config['mlflow_tracking_uri'])
    mlflow.set_experiment(config["experiment_name"])

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
            name="m1_ray_xgb_ft_demo",
            storage_path=config['checkpoint_dir'],
            checkpoint_config=CheckpointConfig(
                num_to_keep=3,
                checkpoint_frequency=25,
                checkpoint_at_end=True,
            ),
            failure_config=FailureConfig(max_failures=2),
        ),
        callbacks=[FailOnceCallback(fail_at_round=args.fail_at)],
    )

    with mlflow.start_run(run_name=f"ft_demo_fail_at_{args.fail_at}"):
        mlflow.log_params({
            "demo": "fault_tolerance",
            "fail_at_round": args.fail_at,
            "n_estimators": config['model']['n_estimators'],
            "checkpoint_frequency": 25,
            "max_failures": 2,
            "ray_version": ray.__version__,
            "python_version": platform.python_version(),
        })

        start = time.time()
        result = trainer.fit()
        wall = time.time() - start

        with result.checkpoint.as_directory() as ckpt_dir:
            model_path = os.path.join(ckpt_dir, "model.ubj")
            if not os.path.exists(model_path):
                model_path = os.path.join(ckpt_dir, "model.json")
            bst = xgb.Booster()
            bst.load_model(model_path)

            X_test_df = test_pd.drop('label', axis=1)
            y_test = test_pd['label'].values
            dtest = xgb.DMatrix(X_test_df.values, feature_names=list(X_test_df.columns))
            y_pred = bst.predict(dtest).astype(int)

            macro_f1 = f1_score(y_test, y_pred, average="macro", zero_division=0)
            accuracy = accuracy_score(y_test, y_pred)

            mlflow.log_metrics({
                "macro_f1": macro_f1,
                "accuracy": accuracy,
                "wall_time_seconds": wall,
                "recovered_from_failure": 1 if os.path.exists(SENTINEL) else 0,
            })

            print("=" * 60)
            print("FAULT TOLERANCE DEMO SUMMARY")
            print("=" * 60)
            print(f"Injected failure at round : {args.fail_at}")
            print(f"Sentinel file present     : {os.path.exists(SENTINEL)}")
            print(f"Final macro_f1            : {macro_f1:.4f}")
            print(f"Final accuracy            : {accuracy:.4f}")
            print(f"Wall time (incl. retry)   : {wall:.1f}s")
            print("=" * 60)

    ray.shutdown()


if __name__ == "__main__":
    main()
