from typing import Any, Dict, Optional
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from payment_risk.common.config import LakehouseConfig


def compute_daily_fraud_summary(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
) -> Dict[str, Any]:
    silver_tx_table = f"{config.silver_schema}.clean_transactions"
    silver_merchants_table = f"{config.silver_schema}.dim_merchants"
    gold_summary_table = f"{config.gold_schema}.daily_fraud_summary"

    df_tx = spark.table(silver_tx_table)
    if target_batch_date:
        df_tx = df_tx.filter(F.col("batch_date") == target_batch_date)

    df_m = spark.table(silver_merchants_table).select(
        F.col("merchant_id"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("merchant_country"),
    )

    df_joined = df_tx.join(df_m, "merchant_id", "left").withColumn(
        "country",
        F.coalesce(F.col("merchant_country"), F.lit("UNKNOWN")),
    )

    group_cols = [
        "batch_date",
        "merchant_category_code",
        "country",
        "card_brand",
        "payment_channel",
    ]

    df_agg = (
        df_joined.groupBy(*group_cols)
        .agg(
            F.count("tx_id").cast("bigint").alias("total_tx_count"),
            F.coalesce(F.sum("amount_cents"), F.lit(0)).cast("bigint").alias("total_volume_cents"),
            F.sum(F.when(F.col("is_fraud_label") == True, 1).otherwise(0))
            .cast("bigint")
            .alias("fraud_tx_count"),
            F.coalesce(
                F.sum(F.when(F.col("is_fraud_label") == True, F.col("amount_cents")).otherwise(0)),
                F.lit(0),
            )
            .cast("bigint")
            .alias("fraud_volume_cents"),
            F.sum(F.when(F.col("tx_status") != "APPROVED", 1).otherwise(0))
            .cast("bigint")
            .alias("decline_count"),
            F.sum(F.when(F.col("avs_code") != "Y", 1).otherwise(0))
            .cast("bigint")
            .alias("avs_mismatch_count"),
            F.sum(F.when(F.col("cvv_code") != "M", 1).otherwise(0))
            .cast("bigint")
            .alias("cvv_mismatch_count"),
        )
        .withColumn(
            "chargeback_rate_bps",
            F.when(
                F.col("total_volume_cents") > 0,
                (F.col("fraud_volume_cents") / F.col("total_volume_cents")) * 10000.0,
            )
            .otherwise(0.0)
            .cast("double"),
        )
        .withColumn(
            "decline_rate",
            F.when(
                F.col("total_tx_count") > 0,
                F.col("decline_count") / F.col("total_tx_count"),
            )
            .otherwise(0.0)
            .cast("double"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    records_count = df_agg.count()
    if records_count > 0:
        delta_summary = DeltaTable.forName(spark, gold_summary_table)
        match_expr = """
            target.batch_date = source.batch_date
            AND target.merchant_category_code = source.merchant_category_code
            AND target.country = source.country
            AND coalesce(target.card_brand, '') = coalesce(source.card_brand, '')
            AND coalesce(target.payment_channel, '') = coalesce(source.payment_channel, '')
        """
        (
            delta_summary.alias("target")
            .merge(df_agg.alias("source"), match_expr)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return {
        "summary_groups_computed": records_count,
        "batch_date": target_batch_date,
    }


def compute_merchant_risk_monitoring(
    spark: Any,
    config: LakehouseConfig,
    target_date: Optional[str] = None,
) -> Dict[str, Any]:
    silver_tx_table = f"{config.silver_schema}.clean_transactions"
    silver_merchants_table = f"{config.silver_schema}.dim_merchants"
    gold_monitoring_table = f"{config.gold_schema}.merchant_risk_monitoring"

    df_tx = spark.table(silver_tx_table)
    if target_date:
        eval_date = target_date
    else:
        latest = df_tx.agg(F.max("batch_date")).collect()[0][0]
        eval_date = str(latest) if latest else "2026-10-14"

    df_window = df_tx.filter(
        (F.col("batch_date") <= eval_date)
        & (F.col("batch_date") >= F.date_sub(F.to_date(F.lit(eval_date)), 30))
    )

    df_m = spark.table(silver_merchants_table)

    df_metrics = (
        df_window.groupBy("merchant_id")
        .agg(
            F.count("tx_id").cast("bigint").alias("window_tx_count_30d"),
            F.coalesce(F.sum("amount_cents"), F.lit(0)).cast("bigint").alias("window_tx_volume_cents_30d"),
            F.sum(F.when(F.col("is_fraud_label") == True, 1).otherwise(0))
            .cast("bigint")
            .alias("window_chargeback_count_30d"),
            F.coalesce(
                F.sum(F.when(F.col("is_fraud_label") == True, F.col("amount_cents")).otherwise(0)),
                F.lit(0),
            )
            .cast("bigint")
            .alias("window_chargeback_volume_cents_30d"),
        )
    )

    df_final = (
        df_m.join(df_metrics, "merchant_id", "inner")
        .withColumn(
            "dispute_rate_pct",
            F.when(
                F.col("window_tx_count_30d") > 0,
                (F.col("window_chargeback_count_30d") / F.col("window_tx_count_30d")) * 100.0,
            )
            .otherwise(0.0)
            .cast("double"),
        )
        .withColumn(
            "is_excessive_dispute_flag",
            (F.col("dispute_rate_pct") >= 0.9).cast("boolean"),
        )
        .withColumn(
            "is_critical_dispute_flag",
            (F.col("dispute_rate_pct") >= 1.5).cast("boolean"),
        )
        .withColumn(
            "recommended_risk_tier",
            F.when(F.col("dispute_rate_pct") >= 1.5, F.lit("HIGH"))
            .when(F.col("dispute_rate_pct") >= 0.9, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn("current_risk_tier", F.coalesce(F.col("risk_tier"), F.lit("LOW")))
        .withColumn("evaluation_date", F.to_date(F.lit(eval_date)))
        .withColumn("computed_at", F.current_timestamp())
        .select(
            F.col("merchant_id").cast("string"),
            F.col("merchant_name").cast("string"),
            F.col("category").cast("string"),
            F.col("mcc").cast("string"),
            F.col("country").cast("string"),
            F.col("current_risk_tier").cast("string"),
            F.col("evaluation_date").cast("date"),
            F.col("window_tx_count_30d").cast("bigint"),
            F.col("window_tx_volume_cents_30d").cast("bigint"),
            F.col("window_chargeback_count_30d").cast("bigint"),
            F.col("window_chargeback_volume_cents_30d").cast("bigint"),
            F.col("dispute_rate_pct").cast("double"),
            F.col("is_excessive_dispute_flag").cast("boolean"),
            F.col("is_critical_dispute_flag").cast("boolean"),
            F.col("recommended_risk_tier").cast("string"),
            F.col("computed_at").cast("timestamp"),
        )
    )

    count = df_final.count()
    flagged_count = 0
    if count > 0:
        delta_m = DeltaTable.forName(spark, gold_monitoring_table)
        (
            delta_m.alias("target")
            .merge(
                df_final.alias("source"),
                "target.merchant_id = source.merchant_id AND target.evaluation_date = source.evaluation_date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        flagged_count = df_final.filter(F.col("is_excessive_dispute_flag")).count()

    return {
        "merchants_evaluated": count,
        "flagged_excessive_disputes": flagged_count,
        "evaluation_date": eval_date,
    }


def generate_high_risk_alerts(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
) -> Dict[str, Any]:
    silver_tx_table = f"{config.silver_schema}.clean_transactions"
    silver_features_table = f"{config.silver_schema}.customer_risk_features"
    silver_merchants_table = f"{config.silver_schema}.dim_merchants"
    gold_alerts_table = f"{config.gold_schema}.high_risk_alerts"

    df_tx = spark.table(silver_tx_table)
    if target_batch_date:
        df_tx = df_tx.filter(F.col("batch_date") == target_batch_date)

    df_feat = spark.table(silver_features_table).select(
        "tx_id",
        "speed_kmh",
        "distance_from_prev_tx_km",
        "amount_zscore",
        "tx_count_1h",
        "is_geo_velocity_anomaly",
        "is_spend_anomaly",
    )

    df_m = spark.table(silver_merchants_table).select(
        F.col("merchant_id"),
        F.col("merchant_name"),
    )

    df_joined = (
        df_tx.join(df_feat, "tx_id", "inner")
        .join(df_m, "merchant_id", "left")
    )

    cond_geo = F.col("is_geo_velocity_anomaly") == True
    cond_spend = (F.col("is_spend_anomaly") == True) & (F.col("amount_zscore") > 3.0) & (~cond_geo)
    cond_velocity = (
        (F.col("tx_count_1h") >= 5)
        & ((F.col("avs_code") != "Y") | (F.col("cvv_code") != "M"))
        & (~cond_geo)
        & (~cond_spend)
    )

    df_flagged = df_joined.filter(cond_geo | cond_spend | cond_velocity)

    df_alerts = (
        df_flagged.withColumn(
            "alert_type",
            F.when(cond_geo, F.lit("IMPOSSIBLE_TRAVEL"))
            .when(cond_spend, F.lit("SPEND_SURGE"))
            .otherwise(F.lit("VELOCITY_BURST_MISMATCH")),
        )
        .withColumn(
            "severity",
            F.when(cond_geo, F.lit("CRITICAL")).otherwise(F.lit("HIGH")),
        )
        .withColumn(
            "reason",
            F.when(
                cond_geo,
                F.concat(
                    F.lit("Implied speed of "),
                    F.round(F.col("speed_kmh"), 1),
                    F.lit(" km/h over "),
                    F.round(F.col("distance_from_prev_tx_km"), 1),
                    F.lit(" km exceeds physical travel limits"),
                ),
            )
            .when(
                cond_spend,
                F.concat(
                    F.lit("Transaction amount deviated "),
                    F.round(F.col("amount_zscore"), 2),
                    F.lit(" standard deviations from customer 30-day baseline"),
                ),
            )
            .otherwise(
                F.concat(
                    F.lit("High velocity burst ("),
                    F.col("tx_count_1h"),
                    F.lit(" tx/1h) accompanied by billing verification mismatch"),
                )
            ),
        )
        .withColumn(
            "alert_id",
            F.sha2(F.concat_ws("||", F.col("tx_id"), F.col("alert_type")), 256),
        )
        .withColumn("status", F.lit("OPEN"))
        .withColumn("created_at", F.current_timestamp())
        .select(
            F.col("alert_id").cast("string"),
            F.col("tx_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("merchant_id").cast("string"),
            F.col("merchant_name").cast("string"),
            F.col("amount_cents").cast("bigint"),
            F.col("currency").cast("string"),
            F.col("timestamp").cast("timestamp"),
            F.col("batch_date").cast("date"),
            F.col("alert_type").cast("string"),
            F.col("severity").cast("string"),
            F.col("reason").cast("string"),
            F.col("speed_kmh").cast("double"),
            F.col("distance_from_prev_tx_km").cast("double"),
            F.col("amount_zscore").cast("double"),
            F.col("tx_count_1h").cast("int"),
            F.col("status").cast("string"),
            F.col("created_at").cast("timestamp"),
        )
    )

    alert_count = df_alerts.count()
    critical_count = 0
    if alert_count > 0:
        delta_alerts = DeltaTable.forName(spark, gold_alerts_table)
        (
            delta_alerts.alias("target")
            .merge(
                df_alerts.alias("source"),
                "target.alert_id = source.alert_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        critical_count = df_alerts.filter(F.col("severity") == "CRITICAL").count()

    return {
        "alerts_generated": alert_count,
        "critical_alerts": critical_count,
        "batch_date": target_batch_date,
    }
