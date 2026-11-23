"""Bronze Layer Table Schemas, Volumes, and Infrastructure DDL.

Idempotently provisions the catalog, Bronze schema, Unity Catalog storage volumes,
and raw Delta Lake tables with schema evolution and dead-letter rescue capabilities.
"""

from typing import Any
from payment_risk.common.config import LakehouseConfig


def create_bronze_infrastructure(spark: Any, config: LakehouseConfig) -> None:
    """Idempotently initializes the catalog, bronze schema, storage volumes,

    and Bronze raw Delta tables.

    Args:
        spark: Active SparkSession.
        config: LakehouseConfig instance with resolved catalog and schema names.
    """
    catalog = config.catalog_name
    bronze = config.bronze_schema

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze} COMMENT 'Raw immutable ingestion layer with dead-letter rescue';")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze}.landing_zone COMMENT 'Storage for daily settlement batch drops';")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze}.checkpoints COMMENT 'RocksDB checkpoint state for Auto Loader';")

    # High-watermark tracking for incremental RDBMS extracts
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}._metadata_watermarks (
      source_name    STRING NOT NULL,
      last_watermark TIMESTAMP NOT NULL,
      updated_at     TIMESTAMP NOT NULL
    ) USING DELTA;
    """)

    # Raw transaction event stream
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.raw_transactions (
      tx_id                  STRING,
      customer_id            STRING,
      card_token             STRING,
      card_fingerprint       STRING,
      card_brand             STRING,
      card_type              STRING,
      card_last4             STRING,
      card_bin               STRING,
      merchant_id            STRING,
      merchant_category_code STRING,
      amount_cents           BIGINT,
      currency               STRING,
      tx_status              STRING,
      response_code          STRING,
      decline_reason         STRING,
      payment_channel        STRING,
      avs_code               STRING,
      cvv_code               STRING,
      eci                    STRING,
      ip_address             STRING,
      device_fingerprint     STRING,
      latitude               DOUBLE,
      longitude              DOUBLE,
      timestamp              STRING,
      _rescued_data          STRING,
      ingested_at            TIMESTAMP,
      source_file            STRING,
      batch_date             DATE
    )
    USING DELTA
    PARTITIONED BY (batch_date)
    TBLPROPERTIES (
      'delta.autoOptimize.optimizeWrite' = 'true',
      'delta.autoOptimize.autoCompact'   = 'true'
    );
    """)

    # Raw dispute/chargeback stream
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.raw_chargebacks (
      chargeback_id          STRING,
      original_tx_id         STRING,
      reason_code            STRING,
      reason_description     STRING,
      disputed_amount_cents  BIGINT,
      currency               STRING,
      filed_date             STRING,
      resolution             STRING,
      dispute_status         STRING,
      _rescued_data          STRING,
      ingested_at            TIMESTAMP,
      source_file            STRING,
      batch_date             DATE
    )
    USING DELTA
    PARTITIONED BY (batch_date);
    """)

    # Raw merchants from Neon PostgreSQL
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.raw_merchants (
      merchant_id         STRING,
      merchant_name       STRING,
      category            STRING,
      mcc                 STRING,
      country             STRING,
      risk_tier           STRING,
      settlement_currency STRING,
      onboarded_date      STRING,
      is_deleted          BOOLEAN,
      deleted_at          TIMESTAMP,
      created_at          TIMESTAMP,
      updated_at          TIMESTAMP,
      ingested_at         TIMESTAMP
    ) USING DELTA;
    """)

    # Raw customers from Neon PostgreSQL
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.raw_customers (
      customer_id         STRING,
      email               STRING,
      country             STRING,
      risk_tier           STRING,
      account_status      STRING,
      lifetime_tx_count   INT,
      lifetime_fraud_count INT,
      is_deleted          BOOLEAN,
      deleted_at          TIMESTAMP,
      created_at          TIMESTAMP,
      updated_at          TIMESTAMP,
      ingested_at         TIMESTAMP
    ) USING DELTA;
    """)

    # Manifest verification audit log
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.manifest_audit (
      batch_date          DATE,
      total_records       INT,
      valid_records       INT,
      quarantined_records INT,
      total_volume_cents  BIGINT,
      manifest_sha256     STRING,
      bronze_sha256       STRING,
      sha256_match        BOOLEAN,
      row_count_match     BOOLEAN,
      verified_at         TIMESTAMP
    ) USING DELTA;
    """)

    # Operational observability run log
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {bronze}.pipeline_run_log (
      run_id           STRING,
      run_date         DATE,
      stage            STRING,
      status           STRING,
      rows_in          BIGINT,
      rows_out         BIGINT,
      rows_rejected    BIGINT,
      duration_seconds DOUBLE,
      checksum_verified BOOLEAN,
      error_message    STRING,
      created_at       TIMESTAMP
    ) USING DELTA;
    """)
