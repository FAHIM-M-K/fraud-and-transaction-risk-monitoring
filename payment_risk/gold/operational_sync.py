from typing import Any, Dict, List, Optional
from payment_risk.common.config import LakehouseConfig


def get_connection_url(raw_url: str) -> str:
    url = raw_url.strip()
    if url.startswith("jdbc:"):
        url = url[5:]
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def sync_operational_risk_feedback(
    spark: Any,
    config: LakehouseConfig,
    connection_url: str,
    batch_date: Optional[str] = None,
) -> Dict[str, Any]:
    scored_table = f"{config.gold_schema}.scored_transactions"
    monitoring_table = f"{config.gold_schema}.merchant_risk_monitoring"

    df_scored = spark.table(scored_table)
    if batch_date:
        df_scored = df_scored.filter(f"batch_date = '{batch_date}'")

    blocked_customers = [
        str(r[0])
        for r in df_scored.filter("recommended_action = 'BLOCK'")
        .select("customer_id")
        .distinct()
        .collect()
        if r[0]
    ]

    df_monitoring = spark.table(monitoring_table)
    if batch_date:
        df_monitoring = df_monitoring.filter(f"evaluation_date = '{batch_date}'")

    flagged_merchants = [
        str(r[0])
        for r in df_monitoring.filter("is_excessive_dispute_flag = true")
        .select("merchant_id")
        .distinct()
        .collect()
        if r[0]
    ]

    clean_url = get_connection_url(connection_url)
    customers_updated = 0
    merchants_updated = 0

    if not clean_url.startswith("sqlite"):
        try:
            import psycopg2

            conn = psycopg2.connect(clean_url)
            with conn:
                with conn.cursor() as cur:
                    if blocked_customers:
                        cur.execute(
                            """
                            UPDATE customer_profiles
                            SET account_status = 'SUSPENDED',
                                risk_tier = 'BLOCKED',
                                updated_at = CURRENT_TIMESTAMP
                            WHERE customer_id = ANY(%s)
                              AND account_status != 'SUSPENDED'
                            """,
                            (blocked_customers,),
                        )
                        customers_updated = cur.rowcount

                    if flagged_merchants:
                        cur.execute(
                            """
                            UPDATE merchants
                            SET risk_tier = 'HIGH',
                                updated_at = CURRENT_TIMESTAMP
                            WHERE merchant_id = ANY(%s)
                              AND risk_tier != 'HIGH'
                            """,
                            (flagged_merchants,),
                        )
                        merchants_updated = cur.rowcount
            conn.close()

            return {
                "blocked_customers_identified": len(blocked_customers),
                "flagged_merchants_identified": len(flagged_merchants),
                "customers_suspended": customers_updated,
                "merchants_flagged": merchants_updated,
                "batch_date": batch_date,
            }
        except ImportError:
            pass

    from sqlalchemy import create_engine, text, bindparam

    connect_args = {"check_same_thread": False} if clean_url.startswith("sqlite") else {}
    engine = create_engine(clean_url, connect_args=connect_args)

    with engine.begin() as conn:
        if blocked_customers:
            cust_stmt = text(
                """
                UPDATE customer_profiles
                SET account_status = 'SUSPENDED',
                    risk_tier = 'BLOCKED',
                    updated_at = CURRENT_TIMESTAMP
                WHERE customer_id IN :cids
                  AND account_status != 'SUSPENDED'
                """
            ).bindparams(bindparam("cids", expanding=True))
            res_c = conn.execute(cust_stmt, {"cids": blocked_customers})
            customers_updated = res_c.rowcount

        if flagged_merchants:
            merch_stmt = text(
                """
                UPDATE merchants
                SET risk_tier = 'HIGH',
                    updated_at = CURRENT_TIMESTAMP
                WHERE merchant_id IN :mids
                  AND risk_tier != 'HIGH'
                """
            ).bindparams(bindparam("mids", expanding=True))
            res_m = conn.execute(merch_stmt, {"mids": flagged_merchants})
            merchants_updated = res_m.rowcount

    return {
        "blocked_customers_identified": len(blocked_customers),
        "flagged_merchants_identified": len(flagged_merchants),
        "customers_suspended": customers_updated,
        "merchants_flagged": merchants_updated,
        "batch_date": batch_date,
    }
