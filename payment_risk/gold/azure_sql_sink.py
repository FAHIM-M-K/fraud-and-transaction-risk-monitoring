from typing import Any, Dict
from pyspark.sql import DataFrame
from payment_risk.common.config import LakehouseConfig


def get_azure_sql_connection_properties(
    username: str,
    password: str,
    batch_size: int = 10000,
) -> Dict[str, str]:
    return {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "batchsize": str(batch_size),
        "isolationLevel": "READ_UNCOMMITTED",
        "tableLock": "true",
    }


def write_table_to_azure_sql(
    df: DataFrame,
    jdbc_url: str,
    table_name: str,
    connection_properties: Dict[str, str],
    mode: str = "overwrite",
) -> int:
    writer = (
        df.write.format("sqlserver")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", connection_properties["user"])
        .option("password", connection_properties["password"])
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
    jdbc_url = (
        f"jdbc:sqlserver://{server}:1433;"
        f"database={database};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30;"
    )
    props = get_azure_sql_connection_properties(username, password)

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
        count = write_table_to_azure_sql(df, jdbc_url, target_table, props, mode="overwrite")
        results[target_table] = count

    return results
