# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.ml.scoring import score_batch_transactions

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

summary = score_batch_transactions(spark, config, target_date)
print(f"Transactions Scored: {summary['records_scored']} using {summary['model_version']}")
print(f"Decision Actions: {summary['actions_breakdown']}")
