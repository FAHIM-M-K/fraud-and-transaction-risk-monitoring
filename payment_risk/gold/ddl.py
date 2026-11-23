from typing import Any
from payment_risk.common.config import LakehouseConfig


def create_gold_infrastructure(spark: Any, config: LakehouseConfig) -> None:
    catalog = config.catalog_name
    gold = config.gold_schema

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {gold} "
        "COMMENT 'Curated Gold business analytics, merchant risk monitoring, and fraud triage alerts';"
    )

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold}.daily_fraud_summary (
      batch_date                 DATE NOT NULL,
      merchant_category_code     STRING NOT NULL,
      country                    STRING NOT NULL,
      card_brand                 STRING,
      payment_channel            STRING,
      total_tx_count             BIGINT NOT NULL,
      total_volume_cents         BIGINT NOT NULL,
      fraud_tx_count             BIGINT NOT NULL,
      fraud_volume_cents         BIGINT NOT NULL,
      chargeback_rate_bps        DOUBLE,
      decline_count              BIGINT NOT NULL,
      decline_rate               DOUBLE,
      avs_mismatch_count         BIGINT NOT NULL,
      cvv_mismatch_count         BIGINT NOT NULL,
      computed_at                TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (batch_date, merchant_category_code)
    TBLPROPERTIES (
      'delta.enableChangeDataFeed'       = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold}.merchant_risk_monitoring (
      merchant_id                        STRING NOT NULL,
      merchant_name                      STRING NOT NULL,
      category                           STRING,
      mcc                                STRING,
      country                            STRING,
      current_risk_tier                  STRING,
      evaluation_date                    DATE NOT NULL,
      window_tx_count_30d                BIGINT NOT NULL,
      window_tx_volume_cents_30d         BIGINT NOT NULL,
      window_chargeback_count_30d        BIGINT NOT NULL,
      window_chargeback_volume_cents_30d BIGINT NOT NULL,
      dispute_rate_pct                   DOUBLE,
      is_excessive_dispute_flag          BOOLEAN NOT NULL,
      is_critical_dispute_flag           BOOLEAN NOT NULL,
      recommended_risk_tier              STRING NOT NULL,
      computed_at                        TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (merchant_id, evaluation_date)
    TBLPROPERTIES (
      'delta.enableChangeDataFeed'       = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold}.high_risk_alerts (
      alert_id                   STRING NOT NULL,
      tx_id                      STRING NOT NULL,
      customer_id                STRING NOT NULL,
      merchant_id                STRING NOT NULL,
      merchant_name              STRING,
      amount_cents               BIGINT NOT NULL,
      currency                   STRING NOT NULL,
      timestamp                  TIMESTAMP NOT NULL,
      batch_date                 DATE NOT NULL,
      alert_type                 STRING NOT NULL,
      severity                   STRING NOT NULL,
      reason                     STRING NOT NULL,
      speed_kmh                  DOUBLE,
      distance_from_prev_tx_km   DOUBLE,
      amount_zscore              DOUBLE,
      tx_count_1h                INT,
      status                     STRING NOT NULL,
      created_at                 TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (severity, status, batch_date)
    TBLPROPERTIES (
      'delta.enableChangeDataFeed'       = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)
