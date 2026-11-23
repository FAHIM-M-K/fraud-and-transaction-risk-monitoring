from typing import Any

DEFAULT_INITIAL_WATERMARK = "1970-01-01T00:00:00Z"


def get_watermark(
    spark: Any,
    watermark_table: str,
    source_name: str,
    default_watermark: str = DEFAULT_INITIAL_WATERMARK,
) -> str:
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
    if hasattr(new_watermark, "strftime"):
        watermark_str = new_watermark.strftime("%Y-%m-%d %H:%M:%S")
    else:
        watermark_str = str(new_watermark)

    merge_sql = f"""
    MERGE INTO {watermark_table} AS target
    USING (SELECT '{source_name}' AS source_name, TIMESTAMP('{watermark_str}') AS last_watermark) AS source
    ON target.source_name = source.source_name
    WHEN MATCHED THEN
      UPDATE SET target.last_watermark = source.last_watermark, target.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (source_name, last_watermark, updated_at)
      VALUES (source.source_name, source.last_watermark, current_timestamp());
    """
    spark.sql(merge_sql)
