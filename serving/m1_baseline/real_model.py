import os
import tempfile

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
import requests
from sklearn.preprocessing import LabelEncoder

from schemas import CategorySuggestion, M1Input, M1Output

NUMERIC_COLS = [
    "log_abs_amount",
    "day_of_week",
    "day_of_month",
    "month",
    "repeat_count",
    "is_recurring_candidate",
]
TEXT_COL = "merchant"

DEFAULT_TRACKING_URI = "http://129.114.27.211:8000/"
DEFAULT_RUN_ID = "a5b5d167eada409f82afe47491d82e19"
LOGGED_MODEL_ID = "m-020c83518f0545d0897fa9d3ed50bfb3"
MODEL_VERSION = "m1_xgboost_v1"

# 本地重建 LabelEncoder 的训练 CSV（与训练时所用 project_category 列一致）
LOCAL_TRAIN_CSV = os.environ.get(
    "M1_TRAIN_CSV",
    os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "..", "datasets", "categorization_train.csv")
    ),
)

_pipeline = None
_label_encoder = None


def load():
    global _pipeline, _label_encoder
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
    run_id = os.environ.get("M1_RUN_ID", DEFAULT_RUN_ID)
    mlflow.set_tracking_uri(tracking_uri)

    # 先尝试标准方式，失败则用 REST API（和 UI 下载走同一端点）
    try:
        _pipeline = mlflow.sklearn.load_model("models:/m1-categorization/6")
    except Exception:
        _pipeline = _download_and_load(tracking_uri)

    # label_encoder.joblib 是 run artifact，MLflow server proxy 目前 500；
    # 先尝试下载，失败则从本地训练 CSV 重建（sklearn.LabelEncoder 按字母序，
    # 与训练时等价）。
    _label_encoder = None
    try:
        le_path = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path="model/label_encoder.joblib"
        )
        _label_encoder = joblib.load(le_path)
    except Exception as e:
        print(f"[M1] MLflow label_encoder download failed ({e}); rebuilding from {LOCAL_TRAIN_CSV}")
        _label_encoder = _rebuild_label_encoder(LOCAL_TRAIN_CSV)

    return _pipeline, _label_encoder


def _rebuild_label_encoder(train_csv_path: str) -> LabelEncoder:
    df = pd.read_csv(train_csv_path, usecols=["project_category"])
    df = df.dropna(subset=["project_category"])
    le = LabelEncoder()
    le.fit(df["project_category"].astype(str).values)
    print(f"[M1] Rebuilt LabelEncoder locally: {len(le.classes_)} classes -> {list(le.classes_)}")
    return le


def _download_and_load(tracking_uri):
    """通过 REST API 下载整个 model 目录，再用 mlflow 本地加载"""
    dest_dir = tempfile.mkdtemp()
    model_dir = os.path.join(dest_dir, "model")
    os.makedirs(model_dir, exist_ok=True)

    files = ["MLmodel", "model.pkl", "conda.yaml",
             "python_env.yaml", "requirements.txt"]

    url = f"{tracking_uri.rstrip('/')}/ajax-api/2.0/mlflow/logged-models/{LOGGED_MODEL_ID}/artifacts/files"
    for fname in files:
        resp = requests.get(url, params={"artifact_file_path": fname})
        resp.raise_for_status()
        with open(os.path.join(model_dir, fname), "wb") as f:
            f.write(resp.content)

    return mlflow.sklearn.load_model(model_dir)


def _build_row(x: M1Input) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                TEXT_COL: x.merchant,
                "log_abs_amount": x.log_abs_amount,
                "day_of_week": x.day_of_week,
                "day_of_month": x.day_of_month,
                "month": x.month,
                "repeat_count": 0,
                "is_recurring_candidate": 0,
            }
        ]
    )


def predict(x: M1Input) -> M1Output:
    if _pipeline is None or _label_encoder is None:
        raise RuntimeError("Model not loaded. Call real_model.load() at startup.")

    row = _build_row(x)
    proba = _pipeline.predict_proba(row)[0]
    classes = _label_encoder.inverse_transform(_pipeline.classes_)

    order = proba.argsort()[::-1]
    top3 = [
        CategorySuggestion(category=str(classes[i]), confidence=float(proba[i]))
        for i in order[:3]
    ]
    predicted = top3[0].category
    confidence = top3[0].confidence

    return M1Output(
        transaction_id=x.transaction_id,
        synthetic_user_id=x.synthetic_user_id,
        predicted_category=predicted,
        confidence=confidence,
        top_3_suggestions=top3,
    )