# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.silver.cleaning import clean_and_quarantine_transactions

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

summary = clean_and_quarantine_transactions(spark, config, target_date)
print(f"Transactions Cleaned: {summary['valid_count']}, Quarantined: {summary['rejected_count']}")
if summary["rejections_by_code"]:
    print(f"Rejections breakdown: {summary['rejections_by_code']}")
