from typing import Any, Dict, Optional
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from delta.tables import DeltaTable
from payment_risk.common.config import LakehouseConfig
from payment_risk.common.risk_scorer import (
    calculate_composite_risk_score,
    ACTION_APPROVE,
    ACTION_CHALLENGE,
    ACTION_BLOCK,
    TIER_LOW,
    TIER_ELEVATED,
    TIER_CRITICAL,
)
from payment_risk.ml.dataset import (
    NUMERICAL_FEATURES,
    CATEGORICAL_FEATURES,
    prepare_feature_dataframe,
)


def score_batch_transactions(
    spark: Any,
    config: LakehouseConfig,
    target_batch_date: Optional[str] = None,
    model_name_or_uri: Optional[str] = None,
    threshold_challenge: float = 0.35,
    threshold_block: float = 0.70,
) -> Dict[str, Any]:
    gold_scored_table = f"{config.gold_schema}.scored_transactions"
    model_name = model_name_or_uri or f"{config.catalog_name}.ml.fraud_detector"

    df_features = prepare_feature_dataframe(spark, config, target_batch_date)
    record_count = df_features.count()

    if record_count == 0:
        return {"records_scored": 0, "actions_breakdown": {}}

    # Try loading registered MLflow model, or fall back to baseline heuristic probability
    model_version = "v1.0.0-champion"
    try:
        import mlflow
        model_uri = f"models:/{model_name}/latest"
        predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")
        feature_cols = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
        df_scored_prob = df_features.withColumn("ml_fraud_probability", predict_udf(*feature_cols))
    except Exception:
        # Failsafe probability estimation from behavioral risk features if model is not yet deployed
        df_scored_prob = df_features.withColumn(
            "ml_fraud_probability",
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.when(F.col("is_geo_velocity_anomaly") == True, 0.70).otherwise(0.0)
                        + F.when(F.col("amount_zscore") > 3.0, 0.40).otherwise(0.0)
                        + F.when(F.col("velocity_accel_1h_24h") > 3.0, 0.30).otherwise(0.0)
                        + F.when(F.col("avs_code") != "Y", 0.15).otherwise(0.0)
                        + F.when(F.col("cvv_code") != "M", 0.20).otherwise(0.0)
                        + 0.02
                    ),
                ),
            ).cast("double"),
        )
        model_version = "v1.0.0-heuristic-prior"

    # Hybrid decisioning UDF schema
    composite_schema = StructType([
        StructField("composite_score", IntegerType(), False),
        StructField("rule_penalties", IntegerType(), False),
        StructField("risk_tier", StringType(), False),
        StructField("recommended_action", StringType(), False),
    ])

    @F.udf(returnType=composite_schema)
    def apply_hybrid_scoring(ml_prob, is_geo_anomaly, amount_zscore, merchant_risk_tier):
        prob = float(ml_prob) if ml_prob is not None else 0.0
        is_geo = 1 if is_geo_anomaly else 0
        zscore = float(amount_zscore) if amount_zscore is not None else 0.0
        tier = str(merchant_risk_tier) if merchant_risk_tier else "LOW"

        comp_score, penalties, r_tier, action = calculate_composite_risk_score(
            ml_probability=prob,
            is_impossible_travel=is_geo,
            distinct_cards_per_device_10m=1,
            amount_zscore=zscore,
            merchant_risk_tier=tier,
            threshold_challenge=threshold_challenge,
            threshold_block=threshold_block,
        )
        return (comp_score, penalties, r_tier, action)

    df_evaluated = (
        df_scored_prob.withColumn(
            "scoring_output",
            apply_hybrid_scoring(
                F.col("ml_fraud_probability"),
                F.col("is_geo_velocity_anomaly"),
                F.col("amount_zscore"),
                F.col("merchant_risk_tier"),
            ),
        )
        .withColumn("composite_risk_score", F.col("scoring_output.composite_score"))
        .withColumn("rule_penalty_score", F.col("scoring_output.rule_penalties"))
        .withColumn("risk_tier", F.col("scoring_output.risk_tier"))
        .withColumn("recommended_action", F.col("scoring_output.recommended_action"))
        .withColumn(
            "top_risk_drivers",
            F.to_json(
                F.array_compact(
                    F.array(
                        F.when(F.col("is_geo_velocity_anomaly") == True, F.lit("IMPOSSIBLE_TRAVEL")),
                        F.when(F.col("amount_zscore") > 3.0, F.lit("SPEND_ANOMALY")),
                        F.when(F.col("velocity_accel_1h_24h") > 3.0, F.lit("VELOCITY_SURGE")),
                        F.when(F.col("merchant_risk_tier") == "HIGH", F.lit("HIGH_RISK_MERCHANT")),
                        F.when(F.col("cvv_code") != "M", F.lit("CVV_MISMATCH")),
                        F.when(F.col("ml_fraud_probability") >= threshold_block, F.lit("CRITICAL_ML_FRAUD_PROB")),
                    )
                )
            ),
        )
                    )
                )
            ),
        )
        .withColumn("model_version", F.lit(model_version))
        .withColumn("scored_at", F.current_timestamp())
        .select(
            F.col("tx_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("merchant_id").cast("string"),
            F.col("amount_cents").cast("bigint"),
            F.col("currency").cast("string"),
            F.col("timestamp").cast("timestamp"),
            F.col("batch_date").cast("date"),
            F.col("ml_fraud_probability").cast("double"),
            F.col("composite_risk_score").cast("int"),
            F.col("rule_penalty_score").cast("int"),
            F.col("risk_tier").cast("string"),
            F.col("recommended_action").cast("string"),
            F.col("top_risk_drivers").cast("string"),
            F.col("model_version").cast("string"),
            F.col("scored_at").cast("timestamp"),
        )
    )

    delta_target = DeltaTable.forName(spark, gold_scored_table)
    (
        delta_target.alias("target")
        .merge(
            df_evaluated.alias("source"),
            "target.tx_id = source.tx_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    action_counts = (
        df_evaluated.groupBy("recommended_action")
        .count()
        .collect()
    )
    breakdown = {row["recommended_action"]: row["count"] for row in action_counts}

    return {
        "records_scored": record_count,
        "actions_breakdown": breakdown,
        "model_version": model_version,
    }
