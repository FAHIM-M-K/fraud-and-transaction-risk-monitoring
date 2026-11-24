# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install pyyaml --quiet

# COMMAND ----------
import sys
import os

from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.gold.azure_sql_sink import publish_gold_mart_to_azure_sql

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

try:
    dbutils.widgets.text("database", "payment_risk_db", "Azure SQL Database Name")
    db_database = dbutils.widgets.get("database").strip() or "payment_risk_db"
except Exception:
    db_database = "payment_risk_db"

db_server = dbutils.secrets.get(scope="azure_sql", key="server")
db_username = dbutils.secrets.get(scope="azure_sql", key="username")
db_password = dbutils.secrets.get(scope="azure_sql", key="password")

print(f"Publishing Gold Serving Layer to Azure SQL...")
print(f"Target Server:   {db_server}")
print(f"Target Database: {db_database}")

sync_results = publish_gold_mart_to_azure_sql(
    spark=spark,
    config=config,
    server=db_server,
    database=db_database,
    username=db_username,
    password=db_password,
)

print("\n---Azure SQL Sync Summary ---")
for tbl, row_count in sync_results.items():
    print(f"  {tbl:32s} -> {row_count:,} rows")
print("---------------------------------")
