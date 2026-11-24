from typing import Any, Dict
from pyspark.sql import DataFrame
from payment_risk.common.config import LakehouseConfig


def write_table_to_azure_sql(
    df: DataFrame,
    server: str,
    database: str,
    table_name: str,
    username: str,
    password: str,
    mode: str = "overwrite",
) -> int:
    writer = (
        df.write.format("sqlserver")
        .option("host", server)
        .option("port", "1433")
        .option("database", database)
        .option("dbtable", table_name)
        .option("user", username)
        .option("password", password)
    )

    if mode == "overwrite":
        writer = writer.option("truncate", "true").mode("overwrite")
    else:
        writer = writer.mode(mode)

    writer.save()
    return df.count()


def publish_gold_mart_to_azure_sql(
    spark: Any,
    config: LakehouseConfig,
    server: str,
    database: str,
    username: str,
    password: str,
) -> Dict[str, int]:
    results: Dict[str, int] = {}

    tables_to_sync = [
        (f"{config.silver_schema}.dim_merchants", "dbo.dim_merchants"),
        (f"{config.silver_schema}.dim_customers", "dbo.dim_customers"),
        (f"{config.gold_schema}.daily_fraud_summary", "dbo.agg_daily_fraud_summary"),
        (f"{config.gold_schema}.merchant_risk_monitoring", "dbo.agg_merchant_risk_monitoring"),
        (f"{config.gold_schema}.high_risk_alerts", "dbo.fact_high_risk_alerts"),
        (f"{config.gold_schema}.scored_transactions", "dbo.fact_scored_transactions"),
    ]

    for source_table, target_table in tables_to_sync:
        df = spark.table(source_table)
        if "is_current" in df.columns:
            df = df.filter("is_current = true")
        count = write_table_to_azure_sql(
            df=df,
            server=server,
            database=database,
            table_name=target_table,
            username=username,
            password=password,
            mode="overwrite",
        )
        results[target_table] = count

    return results
