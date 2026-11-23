# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.ml.train import train_fraud_detection_model

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

result = train_fraud_detection_model(spark, config)
print(f"Model Training Run ID: {result['run_id']}")
print(f"Registered Model: {result['registered_model_name']}")
print(f"Metrics: PR-AUC={result['metrics']['pr_auc']:.4f}, Optimal Threshold={result['metrics']['optimal_threshold']}")
print(f"Top Features: {result['top_features']}")
