# Databricks notebook source
from payment_risk.common.config import resolve_lakehouse_config
from payment_risk.bronze.ddl import create_bronze_infrastructure
from payment_risk.silver.ddl import create_silver_infrastructure
from payment_risk.gold.ddl import create_gold_infrastructure

config = resolve_lakehouse_config(dbutils if "dbutils" in globals() else None)

create_bronze_infrastructure(spark, config)
print(f"Bronze layer initialized: {config.bronze_schema}")

create_silver_infrastructure(spark, config)
print(f"Silver layer initialized: {config.silver_schema}")

create_gold_infrastructure(spark, config)
print(f"Gold layer initialized: {config.gold_schema}")
