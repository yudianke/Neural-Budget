import pandas as pd
import numpy as np
import mlflow
import yaml
import argparse
import time
import platform
import os
import subprocess
import warnings
import tempfile
from pathlib import Path
from mlflow.tracking import MlflowClient

os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
warnings.filterwarnings('ignore')

USER_COL = 'synthetic_user_id'
DATE_COL = 'year_month'
CATEGORY_COL = 'project_category'
SPEND_COL = 'monthly_spend'

CACHE_DIR = Path(os.environ.get('DATA_CACHE_DIR', '/tmp/nb_data_cache'))
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def log(msg):
    print(f"[M3 {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


def _resolve_path(path):
    if not path.startswith('swift://'):
        return path
    _, rest = path.split('swift://', 1)
    container, object_name = rest.split('/', 1)
    local_path = CACHE_DIR / container / object_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if not local_path.exists():
        log(f"downloading {path} -> {local_path}")
        subprocess.run(['swift', 'download', container, object_name, '-o', str(local_path)], check=True)
    else:
        log(f"using cached {local_path}")
    return str(local_path)


def load_split(path):
    local = _resolve_path(path)
    df = pd.read_csv(local)
    log(f"loaded {len(df):,} rows, {df[USER_COL].nunique()} users, {df[CATEGORY_COL].nunique()} categories")
    df = df.dropna(subset=[CATEGORY_COL, SPEND_COL])
    return df


def build_baseline_table(train_df):
    log("computing per-category population statistics")
    grouped = train_df.groupby(CATEGORY_COL)
    stats = grouped[SPEND_COL].agg(
        mean='mean',
        std='std',
        median='median',
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        n_observations='count',
    ).reset_index()
    n_users = grouped[USER_COL].nunique().reset_index()
    n_users.columns = [CATEGORY_COL, 'n_users']
    stats = stats.merge(n_users, on=CATEGORY_COL)
    stats['std'] = stats['std'].fillna(0.0)
    log(f"built baseline table for {len(stats)} categories")
    return stats


def evaluate_baseline(eval_df, baseline_table):
    log("evaluating baseline against eval split")
    means = baseline_table.set_index(CATEGORY_COL)['mean'].to_dict()
    eval_df = eval_df.copy()
    eval_df['pred_mean'] = eval_df[CATEGORY_COL].map(means)
    eval_df = eval_df.dropna(subset=['pred_mean'])
    eval_df['abs_err'] = (eval_df['pred_mean'] - eval_df[SPEND_COL]).abs()

    per_cat = eval_df.groupby(CATEGORY_COL).agg(
        mae=('abs_err', 'mean'),
        n=('abs_err', 'count'),
    ).reset_index()

    overall_mean_mae = float(eval_df['abs_err'].mean())
    median_of_per_cat_mae = float(per_cat['mae'].median())
    return per_cat, overall_mean_mae, median_of_per_cat_mae


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config_m3.yaml')
    args = parser.parse_args()
    config = load_config(args.config)

    tracking_uri = os.environ.get('MLFLOW_TRACKING_URI', config.get('mlflow_tracking_uri'))
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(config['experiment_name'])

    train_path = os.environ.get('M3_TRAIN_PATH', config['train_path'])
    eval_path = os.environ.get('M3_EVAL_PATH', config['eval_path'])
    gate_median_mae = float(os.environ.get('M3_GATE_MEDIAN_MAE', config.get('quality_gate_median_mae', 1000.0)))
    registered_model_name = config.get('registered_model_name', 'm3-forecast-base')

    log(f"tracking_uri={tracking_uri}")
    log(f"train_path={train_path}")
    log(f"eval_path={eval_path}")
    log(f"gate_median_mae={gate_median_mae}")

    log("=== loading train ===")
    train_df = load_split(train_path)

    log("=== loading eval ===")
    eval_df = load_split(eval_path)

    with mlflow.start_run() as run:
        mlflow.log_params({
            'model_type': 'population_baseline_per_category',
            'model_role': 'cold_start_base',
            'train_path': train_path,
            'eval_path': eval_path,
            'train_rows': len(train_df),
            'eval_rows': len(eval_df),
            'train_users': train_df[USER_COL].nunique(),
            'eval_users': eval_df[USER_COL].nunique(),
            'gate_median_mae': gate_median_mae,
            'python_version': platform.python_version(),
            'platform': platform.platform(),
        })

        start = time.time()
        baseline_table = build_baseline_table(train_df)
        per_cat, overall_mean_mae, median_of_per_cat_mae = evaluate_baseline(eval_df, baseline_table)
        train_time = time.time() - start

        log(f"baseline computed and evaluated in {train_time:.2f}s")
        log(f"overall mean MAE (across all eval rows)   = {overall_mean_mae:.2f}")
        log(f"median of per-category MAE  (gate metric) = {median_of_per_cat_mae:.2f}")
        log("per-category MAE:")
        for _, row in per_cat.iterrows():
            log(f"  {row[CATEGORY_COL]:20s} mae={row['mae']:>10.2f} n={int(row['n'])}")

        mlflow.log_metric('train_time_seconds', train_time)
        mlflow.log_metric('overall_mean_mae', overall_mean_mae)
        mlflow.log_metric('median_of_per_category_mae', median_of_per_cat_mae)
        mlflow.log_metric('n_categories', len(baseline_table))
        for _, row in per_cat.iterrows():
            safe = row[CATEGORY_COL].replace(' ', '_').replace('/', '_')
            mlflow.log_metric(f'mae_{safe}', float(row['mae']))

        with tempfile.TemporaryDirectory() as tmpdir:
            table_path = Path(tmpdir) / 'm3_baseline.parquet'
            baseline_table.to_parquet(table_path, index=False)
            mlflow.log_artifact(str(table_path), artifact_path='baseline')
            csv_path = Path(tmpdir) / 'm3_baseline.csv'
            baseline_table.to_csv(csv_path, index=False)
            mlflow.log_artifact(str(csv_path), artifact_path='baseline')

        passed = median_of_per_cat_mae <= gate_median_mae
        mlflow.set_tag('quality_gate_metric', 'median_of_per_category_mae')
        mlflow.set_tag('quality_gate_passed', str(passed).lower())
        mlflow.set_tag('quality_gate_threshold', str(gate_median_mae))
        mlflow.set_tag('model_role', 'cold_start_base')

        if passed:
            model_uri = f"runs:/{run.info.run_id}/baseline"
            try:
                client = MlflowClient()
                try:
                    client.get_registered_model(registered_model_name)
                except Exception:
                    client.create_registered_model(registered_model_name)
                mv = client.create_model_version(
                    name=registered_model_name,
                    source=model_uri,
                    run_id=run.info.run_id,
                )
                log(f"PASSED gate (median_per_cat_mae={median_of_per_cat_mae:.2f} <= {gate_median_mae}) "
                    f"— registered as '{registered_model_name}' v{mv.version}")
            except Exception as e:
                log(f"PASSED gate but registration failed: {e}")
        else:
            mlflow.set_tag('rejected_by_gate', 'true')
            log(f"FAILED gate (median_per_cat_mae={median_of_per_cat_mae:.2f} > {gate_median_mae}) — NOT registered")

        log(f"done | run_id={run.info.run_id}")


if __name__ == '__main__':
    main()
