# Databricks notebook source
import os
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ingestion import parse_postgres_jdbc_connection

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

# 1. Extract blocked customers and flagged merchants from Gold
df_scored = spark.table(f"{config.gold_schema}.scored_transactions")
blocked_customers = [
    str(r[0])
    for r in df_scored.filter("recommended_action = 'BLOCK'")
    .select("customer_id")
    .distinct()
    .collect()
    if r[0]
]

df_monitoring = spark.table(f"{config.gold_schema}.merchant_risk_monitoring")
flagged_merchants = [
    str(r[0])
    for r in df_monitoring.filter("is_excessive_dispute_flag = true")
    .select("merchant_id")
    .distinct()
    .collect()
    if r[0]
]

print(f"Extracted from Gold: {len(blocked_customers)} blocked customers, {len(flagged_merchants)} flagged merchants")

# 2. Update Neon via JVM JDBC (Zero pip dependencies, native to Databricks)
clean_url, props = parse_postgres_jdbc_connection(jdbc_url)
jvm = spark._jvm
info = jvm.java.util.Properties()
for k, v in props.items():
    info.setProperty(k, v)

conn = jvm.java.sql.DriverManager.getConnection(clean_url, info)
customers_updated = 0
merchants_updated = 0

try:
    stmt = conn.createStatement()
    if blocked_customers:
        cids_sql = "', '".join([c.replace("'", "''") for c in blocked_customers])
        sql_c = f"""
            UPDATE customer_profiles
            SET account_status = 'SUSPENDED',
                risk_tier = 'BLOCKED',
                updated_at = CURRENT_TIMESTAMP
            WHERE customer_id IN ('{cids_sql}')
              AND account_status != 'SUSPENDED'
        """
        customers_updated = stmt.executeUpdate(sql_c)

    if flagged_merchants:
        mids_sql = "', '".join([m.replace("'", "''") for m in flagged_merchants])
        sql_m = f"""
            UPDATE merchants
            SET risk_tier = 'HIGH',
                updated_at = CURRENT_TIMESTAMP
            WHERE merchant_id IN ('{mids_sql}')
              AND risk_tier != 'HIGH'
        """
        merchants_updated = stmt.executeUpdate(sql_m)
finally:
    conn.close()

print(f"Reverse ETL Sync Succeeded:")
print(f"  Suspended in Neon: {customers_updated}")
print(f"  Flagged in Neon:   {merchants_updated}")
