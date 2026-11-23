# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ddl import create_bronze_infrastructure

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)
create_bronze_infrastructure(spark, config)
print(f"Bronze layer initialized: {config.catalog_name}")
