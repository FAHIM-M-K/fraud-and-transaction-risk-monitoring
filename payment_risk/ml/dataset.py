from typing import Any, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from payment_risk.common.config import LakehouseConfig

NUMERICAL_FEATURES: List[str] = [
    "amount_cents",
    "tx_count_1h",
    "spend_cents_1h",
    "tx_count_24h",
    "spend_cents_24h",
    "tx_count_7d",
    "spend_cents_7d",
    "avg_spend_cents_30d",
    "std_spend_cents_30d",
    "amount_zscore",
    "seconds_since_prev_tx",
    "distance_from_prev_tx_km",
    "speed_kmh",
    "velocity_accel_1h_24h",
    "amount_ratio_30d",
    "iso_anomaly_score",
]

CATEGORICAL_FEATURES: List[str] = [
    "merchant_category_code",
    "payment_channel",
    "card_brand",
    "card_type",
    "avs_code",
    "cvv_code",
    "currency",
    "merchant_risk_tier",
]

TARGET_COL: str = "is_fraud"


def prepare_feature_dataframe(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
    maturation_days: int = 0,
) -> DataFrame:
    clean_tx_table = f"{config.silver_schema}.clean_transactions"
    features_table = f"{config.silver_schema}.customer_risk_features"
    merchants_table = f"{config.silver_schema}.dim_merchants"

    df_tx = spark.table(clean_tx_table)
    if target_batch_date:
        df_tx = df_tx.filter(F.col("batch_date") == target_batch_date)

    # Label Maturation Lag Filtering:
    # Chargeback disputes take 30-120 days to mature. Recent transactions without a dispute
    # cannot be treated as confirmed negatives. If maturation_days > 0, exclude immature non-fraud cases.
    if maturation_days > 0:
        max_date_row = df_tx.select(F.max("batch_date")).collect()
        if max_date_row and max_date_row[0][0]:
            max_batch_date = max_date_row[0][0]
            cutoff_date = F.date_sub(F.lit(max_batch_date), maturation_days)
            df_tx = df_tx.filter(
                (F.col("is_fraud_label") == True) | (F.col("batch_date") <= cutoff_date)
            )

    df_feat = spark.table(features_table).select(
        "tx_id",
        "tx_count_1h",
        "spend_cents_1h",
        "tx_count_24h",
        "spend_cents_24h",
        "tx_count_7d",
        "spend_cents_7d",
        "avg_spend_cents_30d",
        "std_spend_cents_30d",
        "amount_zscore",
        "seconds_since_prev_tx",
        "distance_from_prev_tx_km",
        "speed_kmh",
        "is_geo_velocity_anomaly",
    )

    df_m = spark.table(merchants_table).select(
        F.col("merchant_id"),
        F.coalesce(F.col("risk_tier"), F.lit("LOW")).alias("merchant_risk_tier"),
    )

    df_joined = (
        df_tx.join(df_feat, "tx_id", "inner")
        .join(df_m, "merchant_id", "left")
    )

    # Derived engineered features for non-linear interactions
    df_enriched = (
        df_joined.withColumn(
            "velocity_accel_1h_24h",
            F.when(
                F.col("tx_count_24h") > 0,
                F.col("tx_count_1h") / (F.col("tx_count_24h") / 24.0),
            ).otherwise(F.col("tx_count_1h")),
        )
        .withColumn(
            "amount_ratio_30d",
            F.when(
                F.col("avg_spend_cents_30d") > 0,
                F.col("amount_cents") / F.col("avg_spend_cents_30d"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn(
            "is_fraud",
            F.when(F.col("is_fraud_label") == True, 1).otherwise(0),
        )
        .withColumn("merchant_risk_tier", F.coalesce(F.col("merchant_risk_tier"), F.lit("LOW")))
        .withColumn("payment_channel", F.coalesce(F.col("payment_channel"), F.lit("ECOMMERCE")))
        .withColumn("card_brand", F.coalesce(F.col("card_brand"), F.lit("UNKNOWN")))
        .withColumn("card_type", F.coalesce(F.col("card_type"), F.lit("UNKNOWN")))
        .withColumn("avs_code", F.coalesce(F.col("avs_code"), F.lit("U")))
        .withColumn("cvv_code", F.coalesce(F.col("cvv_code"), F.lit("U")))
        .withColumn("currency", F.coalesce(F.col("currency"), F.lit("USD")))
        .withColumn("merchant_category_code", F.coalesce(F.col("merchant_category_code"), F.lit("0000")))
        .withColumn("iso_anomaly_score", F.lit(0.0).cast("double"))
    )

    return df_enriched


def split_oot_dataset(
    df: DataFrame,
    split_date: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame, str]:
    if not split_date:
        dates = (
            df.select("batch_date")
            .distinct()
            .orderBy("batch_date")
            .collect()
        )
        all_dates = [str(r[0]) for r in dates]
        if len(all_dates) > 1:
            split_idx = max(1, int(len(all_dates) * 0.75))
            effective_split = all_dates[split_idx - 1]
        elif len(all_dates) == 1:
            effective_split = all_dates[0]
        else:
            effective_split = "2026-10-14"
    else:
        effective_split = split_date

    df_train = df.filter(F.col("batch_date") <= effective_split)
    df_test = df.filter(F.col("batch_date") > effective_split)

    if df_test.count() == 0:
        # If single batch date available, split by chronological timestamp within the batch
        quantiles = df.stat.approxQuantile("timestamp", [0.75], 0.01)
        if quantiles:
            split_ts = quantiles[0]
            df_train = df.filter(F.col("timestamp") <= F.to_timestamp(F.lit(split_ts)))
            df_test = df.filter(F.col("timestamp") > F.to_timestamp(F.lit(split_ts)))

    return df_train, df_test, effective_split
