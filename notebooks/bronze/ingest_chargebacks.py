# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest Bronze Chargebacks
# MAGIC Thin orchestrator executing Auto Loader streaming ingestion into raw_chargebacks.

# COMMAND ----------
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ingestion import ingest_raw_chargebacks_stream

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
query = ingest_raw_chargebacks_stream(spark, config)
query.awaitTermination()
print(f"Auto Loader stream completed successfully for {config.bronze_schema}.raw_chargebacks.")
