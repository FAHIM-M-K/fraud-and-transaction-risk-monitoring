"""High-watermark state management for incremental ingestion.

Provides idempotent reads and MERGE updates on Delta Lake watermark metadata tables.
"""

from datetime import datetime, timezone
from typing import Any

DEFAULT_INITIAL_WATERMARK = "1970-01-01T00:00:00Z"


def get_watermark(
    spark: Any,
    watermark_table: str,
    source_name: str,
    default_watermark: str = DEFAULT_INITIAL_WATERMARK,
) -> str:
    """Retrieves the last recorded high-watermark timestamp for an incremental source.

    Args:
        spark: Active SparkSession.
        watermark_table: Fully qualified Delta table name storing watermarks.
        source_name: Unique identifier for the source feed (e.g. 'neon_merchants').
        default_watermark: Fallback timestamp string if no prior watermark exists.

    Returns:
        ISO timestamp string representing the last successfully committed watermark.
    """
    try:
        df = spark.table(watermark_table)
        match = df.filter(df.source_name == source_name).select("last_watermark").collect()
        if match and match[0][0]:
            val = match[0][0]
            if hasattr(val, "strftime"):
                return val.strftime("%Y-%m-%d %H:%M:%S")
            return str(val)
    except Exception:
        pass
    return default_watermark


def update_watermark(
    spark: Any,
    watermark_table: str,
    source_name: str,
    new_watermark: Any,
) -> None:
    """Persists updated high-watermark using an idempotent Delta Lake MERGE statement.

    Args:
        spark: Active SparkSession.
        watermark_table: Fully qualified Delta table name storing watermarks.
        source_name: Unique identifier for the source feed.
        new_watermark: Datetime or string timestamp representing the new watermark boundary.
    """
    if hasattr(new_watermark, "strftime"):
        watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S")
    else:
        watermark_str = str(new_watermark)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(f"""
        MERGE INTO {watermark_table} AS target
        USING (
            SELECT
                '{source_name}' AS source_name,
                CAST('{watermark_str}' AS TIMESTAMP) AS last_watermark,
                CAST('{now_utc}' AS TIMESTAMP) AS updated_at
        ) AS source
        ON target.source_name = source.source_name
        WHEN MATCHED THEN
            UPDATE SET target.last_watermark = source.last_watermark, target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (source_name, last_watermark, updated_at)
            VALUES (source.source_name, source.last_watermark, source.updated_at)
    """)
