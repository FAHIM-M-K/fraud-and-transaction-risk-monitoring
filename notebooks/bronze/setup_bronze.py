# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Infrastructure Setup
# MAGIC Thin orchestrator initializing Bronze layer schemas, storage volumes, and Delta tables.

# COMMAND ----------
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ddl import create_bronze_infrastructure

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
create_bronze_infrastructure(spark, config)
print(f"Bronze layer infrastructure successfully initialized for catalog '{config.catalog_name}'.")
