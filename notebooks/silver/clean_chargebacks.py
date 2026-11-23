# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.silver.cleaning import clean_and_sync_chargebacks

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
count = clean_and_sync_chargebacks(spark, config)
print(f"Clean Chargebacks Synced: {count}")
