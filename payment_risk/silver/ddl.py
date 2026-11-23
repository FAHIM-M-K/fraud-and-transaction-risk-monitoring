from typing import Any
from payment_risk.common.config import LakehouseConfig


def create_silver_infrastructure(spark: Any, config: LakehouseConfig) -> None:
    catalog = config.catalog_name
    silver = config.silver_schema

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {silver} "
        "COMMENT 'Curated Silver layer with data quality enforcement, deduplication, and risk features';"
    )

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.clean_transactions (
      tx_id                  STRING NOT NULL,
      customer_id            STRING NOT NULL,
      card_token             STRING,
      card_fingerprint       STRING,
      card_brand             STRING,
      card_type              STRING,
      card_last4             STRING,
      card_bin               STRING,
      merchant_id            STRING NOT NULL,
      merchant_category_code STRING,
      amount_cents           BIGINT NOT NULL,
      currency               STRING NOT NULL,
      tx_status              STRING NOT NULL,
      response_code          STRING,
      decline_reason         STRING,
      payment_channel        STRING,
      avs_code               STRING,
      cvv_code               STRING,
      eci                    STRING,
      ip_address             STRING,
      device_fingerprint     STRING,
      latitude               DOUBLE NOT NULL,
      longitude              DOUBLE NOT NULL,
      timestamp              TIMESTAMP NOT NULL,
      is_fraud_label         BOOLEAN,
      processed_at           TIMESTAMP NOT NULL,
      batch_date             DATE NOT NULL
    )
    USING DELTA
    CLUSTER BY (customer_id, timestamp)
    TBLPROPERTIES (
      'delta.enableChangeDataFeed'       = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.rejected_records (
      rejection_id           STRING NOT NULL,
      source_table           STRING NOT NULL,
      tx_id                  STRING,
      raw_payload            STRING,
      rejection_reason_code  STRING NOT NULL,
      rejection_details      STRING,
      quarantined_at         TIMESTAMP NOT NULL,
      batch_date             DATE
    )
    USING DELTA
    CLUSTER BY (rejection_reason_code, quarantined_at)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.clean_chargebacks (
      chargeback_id          STRING NOT NULL,
      original_tx_id         STRING NOT NULL,
      reason_code            STRING,
      reason_description     STRING,
      disputed_amount_cents  BIGINT,
      currency               STRING,
      filed_date             DATE,
      resolution             STRING,
      dispute_status         STRING,
      processed_at           TIMESTAMP NOT NULL,
      batch_date             DATE
    )
    USING DELTA
    CLUSTER BY (original_tx_id, filed_date);
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.dim_merchants (
      merchant_id            STRING NOT NULL,
      merchant_name          STRING NOT NULL,
      category               STRING,
      mcc                    STRING,
      country                STRING,
      risk_tier              STRING,
      settlement_currency    STRING,
      onboarded_date         DATE,
      is_deleted             BOOLEAN,
      created_at             TIMESTAMP,
      updated_at             TIMESTAMP,
      synced_at              TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (merchant_id);
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.dim_customers (
      customer_id            STRING NOT NULL,
      email                  STRING,
      country                STRING,
      risk_tier              STRING,
      account_status         STRING,
      lifetime_tx_count      INT,
      lifetime_fraud_count   INT,
      is_deleted             BOOLEAN,
      created_at             TIMESTAMP,
      updated_at             TIMESTAMP,
      synced_at              TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (customer_id);
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver}.customer_risk_features (
      tx_id                      STRING NOT NULL,
      customer_id                STRING NOT NULL,
      timestamp                  TIMESTAMP NOT NULL,
      amount_cents               BIGINT NOT NULL,
      tx_count_1h                INT NOT NULL,
      spend_cents_1h             BIGINT NOT NULL,
      tx_count_24h               INT NOT NULL,
      spend_cents_24h            BIGINT NOT NULL,
      tx_count_7d                INT NOT NULL,
      spend_cents_7d             BIGINT NOT NULL,
      avg_spend_cents_30d        DOUBLE,
      std_spend_cents_30d        DOUBLE,
      amount_zscore              DOUBLE,
      seconds_since_prev_tx      DOUBLE,
      distance_from_prev_tx_km   DOUBLE,
      speed_kmh                  DOUBLE,
      is_geo_velocity_anomaly    BOOLEAN NOT NULL,
      is_spend_anomaly           BOOLEAN NOT NULL,
      computed_at                TIMESTAMP NOT NULL
    )
    USING DELTA
    CLUSTER BY (customer_id, timestamp)
    TBLPROPERTIES (
      'delta.enableChangeDataFeed'       = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)
