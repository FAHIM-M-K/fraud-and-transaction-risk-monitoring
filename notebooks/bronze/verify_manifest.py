# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Verify Bronze Manifest
# MAGIC Thin orchestrator reconciling Bronze table partition row counts and SHA-256 against Landing Zone manifest.

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
from payment_risk.bronze.verification import verify_bronze_manifest_audit

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

if "dbutils" in globals():
    dbutils.widgets.text("batch_date", "2026-10-14", "Batch Date (YYYY-MM-DD)")
    target_date = dbutils.widgets.get("batch_date").strip()
else:
    target_date = "2026-10-14"

result = verify_bronze_manifest_audit(spark, config, target_date)
print(f"Reconciliation Summary for {target_date}:")
print(f"- Manifest records: {result['total_records']}")
print(f"- Bronze rows:     {result['bronze_count']}")
print(f"- Count match:     {result['count_match']}")
print(f"- SHA-256 match:   {result['sha_match']}")
if not result["count_match"]:
    raise AssertionError(f"Count mismatch: Bronze {result['bronze_count']} != Manifest {result['total_records']}")
