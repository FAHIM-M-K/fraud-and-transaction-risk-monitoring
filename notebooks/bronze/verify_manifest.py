# Databricks notebook source
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
