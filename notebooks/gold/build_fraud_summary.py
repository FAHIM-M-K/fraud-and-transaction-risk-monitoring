# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.gold.aggregations import compute_daily_fraud_summary

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

summary = compute_daily_fraud_summary(spark, config, target_date)
print(f"Daily Fraud Summary computed: {summary['summary_groups_computed']} groups")
