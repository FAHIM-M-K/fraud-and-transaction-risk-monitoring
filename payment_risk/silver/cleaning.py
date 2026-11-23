from typing import Any, Dict, Optional
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from payment_risk.common.config import LakehouseConfig
from payment_risk.common.validators import (
    VALID_CURRENCIES,
    REJECT_ERR_NULL_TX_ID,
    REJECT_ERR_NEGATIVE_AMOUNT,
    REJECT_ERR_INVALID_CURRENCY,
    REJECT_ERR_MALFORMED_TIMESTAMP,
    REJECT_ERR_INVALID_GEO,
    REJECT_ERR_DUPLICATE_TX,
)


def clean_and_quarantine_transactions(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
) -> Dict[str, Any]:
    bronze_tx_table = f"{config.bronze_schema}.raw_transactions"
    bronze_cb_table = f"{config.bronze_schema}.raw_chargebacks"
    silver_tx_table = f"{config.silver_schema}.clean_transactions"
    quarantine_table = f"{config.silver_schema}.rejected_records"

    df_raw = spark.table(bronze_tx_table)
    if target_batch_date:
        df_raw = df_raw.filter(F.col("batch_date") == target_batch_date)

    df_parsed = df_raw.withColumn("parsed_timestamp", F.expr("try_cast(timestamp AS timestamp)"))

    tx_window = Window.partitionBy("tx_id").orderBy(F.col("parsed_timestamp").asc_nulls_last())
    df_flagged = df_parsed.withColumn("tx_row_num", F.row_number().over(tx_window))

    rule_null_id = F.col("tx_id").isNull() | (F.trim(F.col("tx_id")) == "")
    rule_neg_amount = F.col("amount_cents").isNull() | (F.col("amount_cents") < 0)
    rule_invalid_curr = (
        F.col("currency").isNull()
        | (~F.upper(F.col("currency")).isin(list(VALID_CURRENCIES)))
    )
    rule_malformed_ts = F.col("parsed_timestamp").isNull()
    rule_invalid_geo = (
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < -90.0)
        | (F.col("latitude") > 90.0)
        | (F.col("longitude") < -180.0)
        | (F.col("longitude") > 180.0)
    )
    rule_duplicate = (F.col("tx_row_num") > 1) & (~rule_null_id)

    df_evaluated = df_flagged.withColumn(
        "rejection_reason_code",
        F.when(rule_null_id, REJECT_ERR_NULL_TX_ID)
        .when(rule_neg_amount, REJECT_ERR_NEGATIVE_AMOUNT)
        .when(rule_invalid_curr, REJECT_ERR_INVALID_CURRENCY)
        .when(rule_malformed_ts, REJECT_ERR_MALFORMED_TIMESTAMP)
        .when(rule_invalid_geo, REJECT_ERR_INVALID_GEO)
        .when(rule_duplicate, REJECT_ERR_DUPLICATE_TX)
        .otherwise(F.lit(None).cast("string")),
    )

    df_clean = df_evaluated.filter(F.col("rejection_reason_code").isNull())
    df_rejected = df_evaluated.filter(F.col("rejection_reason_code").isNotNull())

    rejected_count = df_rejected.count()
    rejections_by_code: Dict[str, int] = {}

    if rejected_count > 0:
        quarantine_df = df_rejected.select(
            F.expr("uuid()").alias("rejection_id"),
            F.lit(bronze_tx_table).alias("source_table"),
            F.col("tx_id"),
            F.to_json(F.struct(*[F.col(c) for c in df_raw.columns])).alias("raw_payload"),
            F.col("rejection_reason_code"),
            F.concat_ws(
                "; ",
                F.when(rule_null_id, "Missing or blank tx_id"),
                F.when(rule_neg_amount, "Negative or null transaction amount"),
                F.when(rule_invalid_curr, "Currency not in supported whitelist"),
                F.when(rule_malformed_ts, "Malformed ISO-8601 timestamp"),
                F.when(rule_invalid_geo, "Geolocation coordinates outside WGS-84 bounds"),
                F.when(rule_duplicate, "Duplicate tx_id detected in batch window"),
            ).alias("rejection_details"),
            F.current_timestamp().alias("quarantined_at"),
            F.expr("try_cast(batch_date AS date)").alias("batch_date"),
        )
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

        code_counts = (
            df_rejected.groupBy("rejection_reason_code")
            .count()
            .collect()
        )
        rejections_by_code = {row["rejection_reason_code"]: row["count"] for row in code_counts}

    clean_count = df_clean.count()
    if clean_count > 0:
        df_cb = spark.table(bronze_cb_table).select(
            F.col("original_tx_id").alias("cb_tx_id")
        ).distinct()

        clean_cols = [
            "tx_id", "customer_id", "card_token", "card_fingerprint", "card_brand",
            "card_type", "card_last4", "card_bin", "merchant_id", "merchant_category_code",
            "amount_cents", "currency", "tx_status", "response_code", "decline_reason",
            "payment_channel", "avs_code", "cvv_code", "eci", "ip_address",
            "device_fingerprint", "latitude", "longitude", "timestamp", "is_fraud_label",
            "processed_at", "batch_date"
        ]

        df_enriched = (
            df_clean.join(df_cb, df_clean.tx_id == df_cb.cb_tx_id, "left")
            .withColumn("is_fraud_label", F.col("cb_tx_id").isNotNull())
            .withColumn("timestamp", F.col("parsed_timestamp"))
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("batch_date", F.expr("try_cast(batch_date AS date)"))
            .select(*clean_cols)
        )

        delta_silver_tx = DeltaTable.forName(spark, silver_tx_table)
        (
            delta_silver_tx.alias("target")
            .merge(
                df_enriched.alias("source"),
                "target.tx_id = source.tx_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return {
        "valid_count": clean_count,
        "rejected_count": rejected_count,
        "rejections_by_code": rejections_by_code,
    }


def clean_and_sync_chargebacks(
    spark: Any,
    config: LakehouseConfig,
) -> int:
    bronze_cb_table = f"{config.bronze_schema}.raw_chargebacks"
    silver_cb_table = f"{config.silver_schema}.clean_chargebacks"

    df_raw = spark.table(bronze_cb_table)
    cb_window = Window.partitionBy("chargeback_id").orderBy(F.col("ingested_at").desc())

    df_clean = (
        df_raw.withColumn("row_num", F.row_number().over(cb_window))
        .filter(F.col("row_num") == 1)
        .withColumn("filed_date", F.expr("try_cast(filed_date AS date)"))
        .withColumn("batch_date", F.expr("try_cast(batch_date AS date)"))
        .withColumn("disputed_amount_cents", F.expr("try_cast(disputed_amount_cents AS bigint)"))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "chargeback_id",
            "original_tx_id",
            "reason_code",
            "reason_description",
            "disputed_amount_cents",
            "currency",
            "filed_date",
            "resolution",
            "dispute_status",
            "processed_at",
            "batch_date",
        )
    )

    count = df_clean.count()
    if count > 0:
        delta_cb = DeltaTable.forName(spark, silver_cb_table)
        (
            delta_cb.alias("target")
            .merge(
                df_clean.alias("source"),
                "target.chargeback_id = source.chargeback_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return count


def sync_silver_dimensions(
    spark: Any,
    config: LakehouseConfig,
) -> Dict[str, int]:
    bronze_merchants = f"{config.bronze_schema}.raw_merchants"
    silver_merchants = f"{config.silver_schema}.dim_merchants"

    m_window = Window.partitionBy("merchant_id").orderBy(F.col("updated_at").desc())
    df_m = (
        spark.table(bronze_merchants)
        .withColumn("rn", F.row_number().over(m_window))
        .filter(F.col("rn") == 1)
        .withColumn("onboarded_date", F.expr("try_cast(onboarded_date AS date)"))
        .withColumn("synced_at", F.current_timestamp())
        .select(
            "merchant_id",
            "merchant_name",
            "category",
            "mcc",
            "country",
            "risk_tier",
            "settlement_currency",
            "onboarded_date",
            "is_deleted",
            "created_at",
            "updated_at",
            "synced_at",
        )
    )

    m_count = df_m.count()
    if m_count > 0:
        delta_m = DeltaTable.forName(spark, silver_merchants)
        (
            delta_m.alias("target")
            .merge(
                df_m.alias("source"),
                "target.merchant_id = source.merchant_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    bronze_customers = f"{config.bronze_schema}.raw_customers"
    silver_customers = f"{config.silver_schema}.dim_customers"

    c_window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    df_c = (
        spark.table(bronze_customers)
        .withColumn("rn", F.row_number().over(c_window))
        .filter(F.col("rn") == 1)
        .withColumn("synced_at", F.current_timestamp())
        .select(
            "customer_id",
            "email",
            "country",
            "risk_tier",
            "account_status",
            "lifetime_tx_count",
            "lifetime_fraud_count",
            "is_deleted",
            "created_at",
            "updated_at",
            "synced_at",
        )
    )

    c_count = df_c.count()
    if c_count > 0:
        delta_c = DeltaTable.forName(spark, silver_customers)
        (
            delta_c.alias("target")
            .merge(
                df_c.alias("source"),
                "target.customer_id = source.customer_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return {
        "merchants_synced": m_count,
        "customers_synced": c_count,
    }
