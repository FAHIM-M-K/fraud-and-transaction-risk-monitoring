# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.silver.cleaning import sync_silver_dimensions

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
counts = sync_silver_dimensions(spark, config)
print(f"Dimensions Synced - Merchants: {counts['merchants_synced']}, Customers: {counts['customers_synced']}")
