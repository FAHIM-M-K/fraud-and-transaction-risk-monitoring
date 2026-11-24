# Databricks notebook source
# MAGIC %pip install -q lightgbm xgboost scikit-learn

# COMMAND ----------
import os
os.environ["MLFLOW_REGISTRY_URI"] = "databricks-uc"

from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.ml.train import train_fraud_detection_model

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

result = train_fraud_detection_model(spark, config)
print(f"Bake-Off Champion: {result['champion']}")
print(f"Model Training Run ID: {result['run_id']}")
print(f"Registered Model: {result['registered_model_name']}")
print(f"Champion PR-AUC: {result['metrics']['pr_auc']:.4f}")
print(f"Champion Dollar Recall @ 90%: {result['metrics']['dollar_weighted_recall_at_90']:.4f}")
print(f"Optimal Thresholds: Challenge={result['metrics']['threshold_low_challenge']}, Block={result['metrics']['threshold_high_block']}")
print(f"Dollars Saved (cents): {result['metrics']['dollars_saved_cents']}")
print(f"Top Features: {result['top_features']}")
