# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.gold.aggregations import compute_merchant_risk_monitoring

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

eval_date = None
if "dbutils" in globals():
    dbutils.widgets.text("evaluation_date", "", "Evaluation Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("evaluation_date").strip()
    if val:
        eval_date = val

summary = compute_merchant_risk_monitoring(spark, config, eval_date)
print(f"Merchant Risk Monitoring: {summary['merchants_evaluated']} evaluated, {summary['flagged_excessive_disputes']} excessive disputes flagged")
