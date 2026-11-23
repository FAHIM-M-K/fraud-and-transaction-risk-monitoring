# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.silver.features import compute_silver_risk_features

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

results = compute_silver_risk_features(spark, config, target_date)
print(f"Features Computed: {results['features_computed']}")
print(f"Geo-Velocity Anomalies: {results['geo_velocity_anomalies']}, Spend Anomalies: {results['spend_anomalies']}")
