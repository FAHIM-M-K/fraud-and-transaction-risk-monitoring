from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlsplit
from pyspark.sql import functions as F
from payment_risk.common.config import LakehouseConfig
from payment_risk.common.watermark import get_watermark, update_watermark


def parse_postgres_jdbc_connection(raw_url: str) -> Tuple[str, Dict[str, str]]:
    """Normalizes PostgreSQL connection URI into standard JDBC URL and driver properties."""
    url = raw_url.strip()
    if url.startswith("jdbc:"):
        url = url[5:]
    p = urlsplit(url)
    host = p.hostname or "localhost"
    port = f":{p.port}" if p.port else ""
    dbname = p.path.lstrip("/")
    clean_url = f"jdbc:postgresql://{host}{port}/{dbname}"

    props: Dict[str, str] = {
        "driver": "org.postgresql.Driver",
        "sslmode": "require",
    }
    if p.username:
        props["user"] = p.username
    if p.password:
        props["password"] = p.password
    if p.query:
        params = parse_qs(p.query)
        for k, v in params.items():
            if v:
                props[k] = v[0]
    return clean_url, props


def ingest_raw_transactions_stream(spark: Any, config: LakehouseConfig) -> Any:
    landing_path = f"{config.landing_volume_path}/transactions"
    checkpoint_path = f"{config.checkpoints_volume_path}/transactions"
    schema_path = f"{config.checkpoints_volume_path}/transactions_schema"
    target_table = f"{config.bronze_schema}.raw_transactions"

    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFileAge", "30 days")
        .load(landing_path)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn(
            "batch_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"transactions_(\d{4}-\d{2}-\d{2})\.jsonl",
                    1,
                )
            ),
        )
    )

    query = (
        df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query


def ingest_raw_chargebacks_stream(spark: Any, config: LakehouseConfig) -> Any:
    landing_path = f"{config.landing_volume_path}/chargebacks"
    checkpoint_path = f"{config.checkpoints_volume_path}/chargebacks"
    schema_path = f"{config.checkpoints_volume_path}/chargebacks_schema"
    target_table = f"{config.bronze_schema}.raw_chargebacks"

    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFileAge", "30 days")
        .load(landing_path)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn(
            "batch_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"chargebacks_(\d{4}-\d{2}-\d{2})\.jsonl",
                    1,
                )
            ),
        )
    )

    query = (
        df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    return query


def ingest_incremental_dimensions(
    spark: Any,
    config: LakehouseConfig,
    jdbc_url: str,
    jdbc_properties: Optional[Dict[str, str]] = None,
) -> Dict[str, int]:
    watermark_table = f"{config.bronze_schema}._metadata_watermarks"
    clean_url, auto_props = parse_postgres_jdbc_connection(jdbc_url)
    props = dict(auto_props)
    if jdbc_properties:
        props.update(jdbc_properties)

    dimension_sources = [
        ("neon_merchants", "merchants", f"{config.bronze_schema}.raw_merchants"),
        ("neon_customers", "customer_profiles", f"{config.bronze_schema}.raw_customers"),
    ]

    counts: Dict[str, int] = {}
    for source_name, pg_table, target_table in dimension_sources:
        last_watermark = get_watermark(spark, watermark_table, source_name)
        query = f"(SELECT * FROM {pg_table} WHERE updated_at > '{last_watermark}') AS delta_extract"

        df_delta = spark.read.jdbc(
            url=clean_url,
            table=query,
            properties=props,
        ).withColumn("ingested_at", F.current_timestamp())

        count = df_delta.count()
        counts[source_name] = count

        if count > 0:
            df_delta.write.format("delta").mode("append").saveAsTable(target_table)
            max_ts = df_delta.agg(F.max("updated_at")).collect()[0][0]
            update_watermark(spark, watermark_table, source_name, max_ts)

    return counts
