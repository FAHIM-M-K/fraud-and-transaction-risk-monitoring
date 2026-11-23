# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.gold.aggregations import generate_high_risk_alerts

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

summary = generate_high_risk_alerts(spark, config, target_date)
print(f"High Risk Alerts generated: {summary['alerts_generated']} total, {summary['critical_alerts']} critical")
