# NeuralBudget M1 Serving App

This folder contains the core FastAPI serving logic for the **M1 Transaction Auto-Categorization** subsystem in NeuralBudget.

## Files

### `schemas.py`
Defines the **input and output data schemas** using Pydantic models.

It specifies:
- the transaction request format (`M1Input`)
- the prediction response format (`M1Output`)
- top-3 category suggestion objects (`CategorySuggestion`)

This ensures the API contract is strongly typed and consistent with the shared JSON interface used by the data and training roles.

---

### `mock_model.py`
Implements a **mock inference model** for the initial implementation milestone.

This file simulates category prediction logic using simple merchant-name and historical-category rules.

It is used to:
- validate the serving workflow
- test API correctness
- support independent subsystem delivery before full model integration

The real trained model from the training pipeline can later replace this file with minimal API changes.

---

### `server.py`
Implements the **FastAPI application and HTTP endpoints**.

Endpoints:
- `GET /health` → service health check
- `POST /predict/category` → M1 category prediction endpoint

This file connects:
- request validation from `schemas.py`
- prediction logic from `mock_model.py`

and exposes the serving subsystem as a runnable API service.

---

## Purpose
These three files together form the **role-owned serving subsystem** for the project's initial implementation milestone.
The subsystem is intentionally modular so the mock model can later be replaced by a trained XGBoost or other production model.
