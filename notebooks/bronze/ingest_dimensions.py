# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Ingest Bronze Dimensions
# MAGIC Thin orchestrator querying Neon PostgreSQL incrementally with watermark state tracking.

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
from payment_risk.bronze.ingestion import ingest_incremental_dimensions

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

# Resolve Neon credentials (Secret Scope -> Widget -> Environment Variable)
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
    raise ValueError(
        f"Neon PostgreSQL connection URL required. Set 'neon_jdbc_url' in Databricks secret scope '{config.secret_scope}' "
        "or pass widget 'neon_jdbc_url'."
    )

if jdbc_url.startswith("postgresql://"):
    jdbc_url = jdbc_url.replace("postgresql://", "jdbc:postgresql://", 1)

counts = ingest_incremental_dimensions(spark, config, jdbc_url)
for src, count in counts.items():
    print(f"Ingested {count} records for {src}.")
