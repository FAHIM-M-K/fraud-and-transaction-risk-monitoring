# Databricks notebook source
# MAGIC %pip install -q psycopg2-binary

# COMMAND ----------
import os
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.gold.operational_sync import sync_operational_risk_feedback

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

jdbc_url = None
if "dbutils" in globals():
    try:
        jdbc_url = dbutils.secrets.get(config.secret_scope, "neon_jdbc_url")
    except Exception:
        pass
    if not jdbc_url:
        dbutils.widgets.text("neon_jdbc_url", "", "Neon JDBC URL")
        jdbc_url = dbutils.widgets.get("neon_jdbc_url").strip()

if not jdbc_url:
    jdbc_url = os.environ.get("POSTGRES_URL") or os.environ.get("NEON_JDBC_URL")

if not jdbc_url:
    raise ValueError(f"neon_jdbc_url required in secret scope '{config.secret_scope}' or widget")

target_date = None
if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "", "Batch Date (Optional YYYY-MM-DD)")
    val = dbutils.widgets.get("batch_date").strip()
    if val:
        target_date = val

summary = sync_operational_risk_feedback(spark, config, jdbc_url, target_date)
print(f"Reverse ETL Feedback Sync Completed for batch date: {summary['batch_date'] or 'ALL'}")
print(f"  Blocked Customers Identified: {summary['blocked_customers_identified']}")
print(f"  Accounts Suspended in Neon:   {summary['customers_suspended']}")
print(f"  Merchants Flagged to HIGH:    {summary['merchants_flagged']}")
