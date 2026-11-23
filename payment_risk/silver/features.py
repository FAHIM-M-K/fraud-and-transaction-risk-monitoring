from typing import Any, Dict, Optional
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from payment_risk.common.config import LakehouseConfig


def compute_silver_risk_features(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
) -> Dict[str, Any]:
    clean_tx_table = f"{config.silver_schema}.clean_transactions"
    features_table = f"{config.silver_schema}.customer_risk_features"

    df_clean = spark.table(clean_tx_table)
    if target_batch_date:
        df_clean = df_clean.filter(
            (F.col("batch_date") <= target_batch_date)
            & (F.col("batch_date") >= F.date_sub(F.to_date(F.lit(target_batch_date)), 35))
        )

    df_with_epoch = df_clean.withColumn("epoch_sec", F.col("timestamp").cast("long"))

    w_row = Window.partitionBy("customer_id").orderBy("epoch_sec", "tx_id")
    w_1h = Window.partitionBy("customer_id").orderBy("epoch_sec").rangeBetween(-3600, 0)
    w_24h = Window.partitionBy("customer_id").orderBy("epoch_sec").rangeBetween(-86400, 0)
    w_7d = Window.partitionBy("customer_id").orderBy("epoch_sec").rangeBetween(-604800, 0)
    w_30d = Window.partitionBy("customer_id").orderBy("epoch_sec").rangeBetween(-2592000, 0)

    df_velocity = (
        df_with_epoch.withColumn("tx_count_1h", F.count("tx_id").over(w_1h))
        .withColumn("spend_cents_1h", F.coalesce(F.sum("amount_cents").over(w_1h), F.lit(0)))
        .withColumn("tx_count_24h", F.count("tx_id").over(w_24h))
        .withColumn("spend_cents_24h", F.coalesce(F.sum("amount_cents").over(w_24h), F.lit(0)))
        .withColumn("tx_count_7d", F.count("tx_id").over(w_7d))
        .withColumn("spend_cents_7d", F.coalesce(F.sum("amount_cents").over(w_7d), F.lit(0)))
        .withColumn("avg_spend_cents_30d", F.avg("amount_cents").over(w_30d))
        .withColumn("std_spend_cents_30d", F.stddev("amount_cents").over(w_30d))
    )

    df_zscore = df_velocity.withColumn(
        "amount_zscore",
        F.when(
            F.col("std_spend_cents_30d").isNotNull() & (F.col("std_spend_cents_30d") > 0.0),
            (F.col("amount_cents") - F.col("avg_spend_cents_30d")) / F.col("std_spend_cents_30d"),
        ).otherwise(F.lit(0.0)),
    ).withColumn("is_spend_anomaly", F.col("amount_zscore") > 3.0)

    df_geo_lag = (
        df_zscore.withColumn("prev_epoch", F.lag("epoch_sec", 1).over(w_row))
        .withColumn("prev_lat", F.lag("latitude", 1).over(w_row))
        .withColumn("prev_lon", F.lag("longitude", 1).over(w_row))
    )

    df_geo_delta = df_geo_lag.withColumn(
        "seconds_since_prev_tx",
        F.when(
            F.col("prev_epoch").isNotNull(),
            F.col("epoch_sec") - F.col("prev_epoch"),
        ).otherwise(F.lit(0.0)),
    )

    # Great-circle distance using Haversine formula
    dlat = F.radians(F.col("latitude") - F.col("prev_lat"))
    dlon = F.radians(F.col("longitude") - F.col("prev_lon"))
    haversine_a = (
        F.pow(F.sin(dlat / 2.0), 2)
        + F.cos(F.radians(F.col("prev_lat")))
        * F.cos(F.radians(F.col("latitude")))
        * F.pow(F.sin(dlon / 2.0), 2)
    )
    haversine_c = 2.0 * F.asin(
        F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), haversine_a)))
    )

    df_dist = df_geo_delta.withColumn(
        "distance_from_prev_tx_km",
        F.when(
            F.col("prev_lat").isNotNull() & F.col("prev_lon").isNotNull(),
            6371.0 * haversine_c,
        ).otherwise(F.lit(0.0)),
    )

    # Speeds > 800 km/h with distance > 100km indicate impossible travel anomalies
    df_speed = df_dist.withColumn(
        "speed_kmh",
        F.when(
            (F.col("seconds_since_prev_tx") > 0.0) & (F.col("distance_from_prev_tx_km") > 0.0),
            F.col("distance_from_prev_tx_km") / (F.col("seconds_since_prev_tx") / 3600.0),
        ).otherwise(F.lit(0.0)),
    ).withColumn(
        "is_geo_velocity_anomaly",
        (F.col("speed_kmh") > 800.0)
        & (F.col("distance_from_prev_tx_km") > 100.0)
        & (F.col("seconds_since_prev_tx") > 0.0),
    )

    if target_batch_date:
        df_speed = df_speed.filter(F.col("batch_date") == target_batch_date)

    df_final_features = df_speed.select(
        F.col("tx_id").cast("string"),
        F.col("customer_id").cast("string"),
        F.col("timestamp").cast("timestamp"),
        F.col("amount_cents").cast("bigint"),
        F.col("tx_count_1h").cast("int"),
        F.col("spend_cents_1h").cast("bigint"),
        F.col("tx_count_24h").cast("int"),
        F.col("spend_cents_24h").cast("bigint"),
        F.col("tx_count_7d").cast("int"),
        F.col("spend_cents_7d").cast("bigint"),
        F.col("avg_spend_cents_30d").cast("double"),
        F.col("std_spend_cents_30d").cast("double"),
        F.col("amount_zscore").cast("double"),
        F.col("seconds_since_prev_tx").cast("double"),
        F.col("distance_from_prev_tx_km").cast("double"),
        F.col("speed_kmh").cast("double"),
        F.col("is_geo_velocity_anomaly").cast("boolean"),
        F.col("is_spend_anomaly").cast("boolean"),
        F.current_timestamp().alias("computed_at"),
    )

    total_count = df_final_features.count()

    if total_count > 0:
        delta_feat = DeltaTable.forName(spark, features_table)
        (
            delta_feat.alias("target")
            .merge(
                df_final_features.alias("source"),
                "target.tx_id = source.tx_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        geo_anomalies = df_final_features.filter(F.col("is_geo_velocity_anomaly")).count()
        spend_anomalies = df_final_features.filter(F.col("is_spend_anomaly")).count()
    else:
        geo_anomalies = 0
        spend_anomalies = 0

    return {
        "features_computed": total_count,
        "geo_velocity_anomalies": geo_anomalies,
        "spend_anomalies": spend_anomalies,
    }
