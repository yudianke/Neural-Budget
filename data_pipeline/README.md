# Neural Budget – Data Pipeline

This repository implements a reproducible data pipeline for a personal finance ML system.  
It ingests external datasets, generates synthetic transaction data, constructs batch datasets for training, and uploads all artifacts to Chameleon object storage.

---

## 📊 Overview

The pipeline supports:

- External data ingestion (CES, MTBI, MoneyData)
- Synthetic data generation
- Batch dataset construction (categorization, anomaly detection, forecasting)
- Online feature computation
- Upload to Chameleon Swift object storage
- Event simulation for inference

