import requests
resp = requests.get(
    "http://129.114.25.192:8000/ajax-api/2.0/mlflow/logged-models/m-ba8572c0ee95493a9affc9b2e0a0eb29/artifacts/files",
    params={"artifact_file_path": "model.pkl"}
)
print(f"{'✅' if resp.ok else '❌'} {resp.status_code}, size: {len(resp.content)} bytes")