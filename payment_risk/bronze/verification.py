import hashlib
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
    manifest_vol = f"{config.landing_volume_path}/manifests"
    manifest_file = f"{manifest_vol}/manifest_{target_date}.json"
    landing_tx_file = f"{config.landing_volume_path}/transactions/transactions_{target_date}.jsonl"

    manifest_data: Dict[str, Any] = {}
    try:
        with open(manifest_file, "r", encoding="utf-8") as f:
            manifest_data = json.load(f)
    except Exception:
        df_m = spark.read.option("multiline", "true").json(manifest_file)
        manifest_data = json.loads(df_m.toJSON().first())

    expected_count = int(manifest_data.get("total_records") or manifest_data.get("record_count", 0))
    expected_sha = str(manifest_data.get("sha256_checksum") or manifest_data.get("payload_sha256", ""))
    total_volume_cents = int(manifest_data.get("total_volume_cents", 0))
    valid_records = int(manifest_data.get("valid_records") or 0)
    quarantined_records = int(manifest_data.get("quarantined_records") or manifest_data.get("corrupted_records", 0))

    bronze_table = f"{config.bronze_schema}.raw_transactions"
    df_bronze = spark.table(bronze_table).filter(F.col("batch_date") == target_date)
    bronze_count = df_bronze.count()

    count_match = bronze_count == expected_count

    sha_match = False
    bronze_sha = ""

    try:
        with open(landing_tx_file, "r", encoding="utf-8") as f:
            raw_records = [json.loads(line) for line in f if line.strip()]
        sha_match, bronze_sha, _ = verify_manifest_checksum(raw_records, expected_sha)
    except Exception:
        if bronze_count > 0:
            meta_cols = {"_rescued_data", "ingested_at", "source_file", "batch_date"}
            records = [
                {k: v for k, v in row.asDict().items() if k not in meta_cols and v is not None}
                for row in df_bronze.collect()
            ]
            sha_match, bronze_sha, _ = verify_manifest_checksum(records, expected_sha)

    audit_row = spark.createDataFrame([
        {
            "batch_date": target_date,
            "total_records": expected_count,
            "valid_records": valid_records if valid_records > 0 else bronze_count,
            "quarantined_records": quarantined_records,
            "total_volume_cents": total_volume_cents,
            "manifest_sha256": expected_sha,
            "bronze_sha256": bronze_sha,
            "sha256_match": sha_match,
            "row_count_match": count_match,
        }
    ]).withColumn("batch_date", F.to_date("batch_date")).withColumn("verified_at", F.current_timestamp())

    audit_table = f"{config.bronze_schema}.manifest_audit"
    audit_row.write.format("delta").mode("append").saveAsTable(audit_table)

    return {
        "batch_date": target_date,
        "total_records": expected_count,
        "bronze_count": bronze_count,
        "count_match": count_match,
        "sha_match": sha_match,
        "manifest_sha": expected_sha,
        "bronze_sha": bronze_sha,
    }
