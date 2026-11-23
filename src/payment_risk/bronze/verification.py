"""Bronze Manifest Reconciliation and Cryptographic Verification.

Reconciles ingested Bronze partition counts and computes payload checksums against
the Landing Zone batch manifest to enforce row-algebra and cryptographic integrity.
"""

import json
from typing import Any, Dict
from pyspark.sql import functions as F
from payment_risk.common.config import LakehouseConfig
from payment_risk.common.audit import verify_manifest_checksum


def verify_bronze_manifest_audit(
    spark: Any,
    config: LakehouseConfig,
    target_date: str,
) -> Dict[str, Any]:
    """Reconciles ingested Bronze partition against the Landing Zone cryptographic manifest.

    Args:
        spark: Active SparkSession.
        config: LakehouseConfig instance.
        target_date: Batch date string in 'YYYY-MM-DD' format.

    Returns:
        Dictionary summary containing count match, SHA-256 match, and audit metrics.
    """
    manifest_vol = f"{config.landing_volume_path}/manifests"
    manifest_file = f"{manifest_vol}/manifest_{target_date}.json"

    # Read manifest: support both POSIX Volume access and multiline Spark DataFrame
    manifest_data: Dict[str, Any] = {}
    try:
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)
    except Exception:
        manifest_df = spark.read.option("multiline", "true").json(manifest_file)
        manifest_data = manifest_df.collect()[0].asDict()

    expected_sha = str(manifest_data.get("sha256_checksum") or manifest_data.get("payload_sha256_hash", ""))
    total_records = int(manifest_data.get("total_records", 0))
    valid_records = int(manifest_data.get("valid_records", total_records))
    quarantined_records = int(manifest_data.get("quarantined_records", 0))
    total_volume = int(manifest_data.get("total_volume_cents", 0))

    bronze_table = f"{config.bronze_schema}.raw_transactions"
    df_bronze = spark.table(bronze_table).filter(F.col("batch_date") == target_date)
    bronze_count = df_bronze.count()

    count_match = (bronze_count == total_records)
    sha_match = False
    bronze_sha = ""

    if bronze_count > 0:
        # Exclude ingested metadata columns when computing row-level digest
        meta_cols = {"_rescued_data", "ingested_at", "source_file", "batch_date"}
        records = [
            {k: v for k, v in row.asDict().items() if k not in meta_cols and v is not None}
            for row in df_bronze.collect()
        ]
        sha_match, bronze_sha, _ = verify_manifest_checksum(records, expected_sha)

    # Persist audit reconciliation outcome
    audit_row = spark.createDataFrame([
        (
            target_date,
            total_records,
            valid_records,
            quarantined_records,
            total_volume,
            expected_sha,
            bronze_sha,
            sha_match,
            count_match,
        )
    ], schema="""
        batch_date STRING,
        total_records INT,
        valid_records INT,
        quarantined_records INT,
        total_volume_cents BIGINT,
        manifest_sha256 STRING,
        bronze_sha256 STRING,
        sha256_match BOOLEAN,
        row_count_match BOOLEAN
    """).withColumn("batch_date", F.to_date("batch_date")).withColumn("verified_at", F.current_timestamp())

    audit_table = f"{config.bronze_schema}.manifest_audit"
    audit_row.write.format("delta").mode("append").saveAsTable(audit_table)

    return {
        "batch_date": target_date,
        "total_records": total_records,
        "bronze_count": bronze_count,
        "count_match": count_match,
        "sha_match": sha_match,
        "expected_sha": expected_sha,
        "computed_sha": bronze_sha,
    }
