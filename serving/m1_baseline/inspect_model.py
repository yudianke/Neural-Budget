import os
import pickle
import tempfile

import requests

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://129.114.27.243:8000/")
LOGGED_MODEL_ID = os.environ.get("M1_LOGGED_MODEL_ID", "m-020c83518f0545d0897fa9d3ed50bfb3")


def fetch(filename: str, dest_dir: str) -> str:
    url = f"{TRACKING_URI.rstrip('/')}/ajax-api/2.0/mlflow/logged-models/{LOGGED_MODEL_ID}/artifacts/files"
    resp = requests.get(url, params={"artifact_file_path": filename})
    resp.raise_for_status()
    local = os.path.join(dest_dir, filename)
    with open(local, "wb") as f:
        f.write(resp.content)
    print(f"  downloaded {filename} ({len(resp.content)} bytes)")
    return local


def main(): 
    dest = tempfile.mkdtemp()
    print(f"Downloading to {dest}")

    mlmodel_path = fetch("MLmodel", dest)
    with open(mlmodel_path, "r", encoding="utf-8") as f:
        mlmodel_text = f.read()
    print("\n=== MLmodel file contents ===")
    print(mlmodel_text)
    print("=== end MLmodel ===\n")

    model_path = fetch("model.pkl", dest)
    with open(model_path, "rb") as f:
        obj = pickle.load(f)

    print(f"\npickle.load returned: {type(obj).__module__}.{type(obj).__name__}")

    if hasattr(obj, "steps"):
        print("It IS an sklearn Pipeline.")
        for name, step in obj.steps:
            print(f"  step '{name}': {type(step).__module__}.{type(step).__name__}")
        if hasattr(obj, "named_steps") and "features" in obj.named_steps:
            feats = obj.named_steps["features"]
            if hasattr(feats, "transformers_"):
                print("  ColumnTransformer transformers_:")
                for name, trans, cols in feats.transformers_:
                    print(f"    - {name}: {type(trans).__name__} on cols={cols}")
        print("\n>>> CASE A: full Pipeline. Fix = bypass mlflow.sklearn.load_model and raw pickle.load.")
    else:
        print("It is NOT a Pipeline (terminal estimator only).")
        print(f"  attrs: n_classes={getattr(obj,'n_classes_',None)} classes_={getattr(obj,'classes_',None)}")
        print("\n>>> CASE B: MLflow logged only the classifier. Fix = local retrain.")


if __name__ == "__main__":
    main()