# Databricks notebook source
# MAGIC %pip install -q pg8000

# COMMAND ----------
import os
import ssl
from urllib.parse import urlsplit
import pg8000.native
from payment_risk.common.config import resolve_lakehouse_config

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

raw = jdbc_url.strip()
if raw.startswith("jdbc:"):
    raw = raw[5:]

p = urlsplit(raw)
host = p.hostname
port = p.port or 5432
user = p.username
password = p.password
database = p.path.lstrip("/")

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

ssl_ctx = ssl.create_default_context()
conn = pg8000.native.Connection(
    user=user,
    password=password,
    host=host,
    port=port,
    database=database,
    ssl_context=ssl_ctx,
)

customers_updated = 0
merchants_updated = 0

try:
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
        conn.run(sql_c)
        res_cnt = conn.run(f"SELECT count(*) FROM customer_profiles WHERE customer_id IN ('{cids_sql}') AND account_status = 'SUSPENDED'")
        customers_updated = res_cnt[0][0] if res_cnt else len(blocked_customers)

    if flagged_merchants:
        mids_sql = "', '".join([m.replace("'", "''") for m in flagged_merchants])
        sql_m = f"""
            UPDATE merchants
            SET risk_tier = 'HIGH',
                updated_at = CURRENT_TIMESTAMP
            WHERE merchant_id IN ('{mids_sql}')
              AND risk_tier != 'HIGH'
        """
        conn.run(sql_m)
        res_mcnt = conn.run(f"SELECT count(*) FROM merchants WHERE merchant_id IN ('{mids_sql}') AND risk_tier = 'HIGH'")
        merchants_updated = res_mcnt[0][0] if res_mcnt else len(flagged_merchants)
finally:
    conn.close()

print(f"Reverse ETL Sync Succeeded:")
print(f"  Suspended in Neon: {customers_updated}")
print(f"  Flagged in Neon:   {merchants_updated}")
