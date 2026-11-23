# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Infrastructure Setup
# MAGIC Thin orchestrator initializing Bronze layer schemas, storage volumes, and Delta tables.

# COMMAND ----------
import os
import sys

# Ensure src/ is on Python sys.path across interactive notebooks, jobs, and workspace repos
candidate_paths = [
    os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")),
    "/Workspace/Users/fahimmeethale@gmail.com/payment_risk/src",
]
if "dbutils" in globals():
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        if "/payment_risk/" in nb_path:
            base = nb_path.split("/payment_risk/")[0] + "/payment_risk/src"
            candidate_paths.extend([f"/Workspace{base}", base])
    except Exception:
        pass

for p in candidate_paths:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ddl import create_bronze_infrastructure

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
create_bronze_infrastructure(spark, config)
print(f"Bronze layer infrastructure successfully initialized for catalog '{config.catalog_name}'.")
