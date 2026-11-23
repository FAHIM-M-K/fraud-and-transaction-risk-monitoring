# Databricks notebook source
import os
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ingestion import ingest_incremental_dimensions

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

counts = ingest_incremental_dimensions(spark, config, jdbc_url)
for src, count in counts.items():
    print(f"Ingested {count} records for {src}")
