# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest Bronze Chargebacks
# MAGIC Thin orchestrator executing Auto Loader streaming ingestion into raw_chargebacks.

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
from payment_risk.bronze.ingestion import ingest_raw_chargebacks_stream

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
query = ingest_raw_chargebacks_stream(spark, config)
query.awaitTermination()
print(f"Auto Loader stream completed successfully for {config.bronze_schema}.raw_chargebacks.")
