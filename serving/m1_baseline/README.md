# NeuralBudget M1 Service

This folder now serves the Ray-trained `m1-ray-categorization` model bundle through a stable FastAPI interface used by Actual.

## Files

- `real_model.py`: downloads the latest registered `m1-ray-categorization` bundle from MLflow and performs inference
- `schemas.py`: request, response, and feedback payload schemas
- `server.py`: FastAPI app entrypoint
- `Dockerfile`: container image for Chameleon deployment
- `requirements.txt`: Python dependencies

## Endpoint

- `GET /health`
- `POST /predict/category`
- `POST /feedback`

## Runtime env

- `MLFLOW_TRACKING_URI`: MLflow host
- `M1_REGISTERED_MODEL_NAME`: defaults to `m1-ray-categorization`
- `M1_MODEL_VERSION`: optional explicit registered model version
- `M1_FEEDBACK_LOG_PATH`: JSONL file where accepted/overridden category feedback is appended

## Feedback loop

Actual posts accepted and overridden category choices to `/feedback`. To turn those logs into retraining data for `training/m1_ray/train_m1_ray.py`, run:

```bash
python training/m1_ray/build_feedback_dataset.py \
  --input /path/to/m1_ray_feedback.jsonl \
  --output /path/to/m1_ray_feedback.csv
```

Then retrain with:

```bash
python training/m1_ray/train_m1_ray.py --mode retrain
```

Or run the full compile + retrain loop in one command:

```bash
bash training/m1_ray/run_retrain_loop.sh /path/to/m1_ray_feedback.jsonl /path/to/m1_ray_feedback.csv
```
