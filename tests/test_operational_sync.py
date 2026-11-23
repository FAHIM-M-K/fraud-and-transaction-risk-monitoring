import sqlite3
import pytest
from unittest.mock import MagicMock
from payment_risk.common.config import LakehouseConfig
from payment_risk.gold.operational_sync import (
    get_connection_url,
    sync_operational_risk_feedback,
)


def test_get_connection_url():
    raw_jdbc = "jdbc:postgresql://user:pass@host:5432/db?sslmode=require"
    assert get_connection_url(raw_jdbc) == "postgresql://user:pass@host:5432/db?sslmode=require"

    raw_legacy = "postgres://user:pass@host:5432/db"
    assert get_connection_url(raw_legacy) == "postgresql://user:pass@host:5432/db"

    sqlite_url = "sqlite:///test.db"
    assert get_connection_url(sqlite_url) == "sqlite:///test.db"


@pytest.fixture
def sqlite_test_db(tmp_path):
    db_file = tmp_path / "test_gateway.db"
    db_url = f"sqlite:///{db_file}"

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE customer_profiles (
            customer_id TEXT PRIMARY KEY,
            account_status TEXT NOT NULL,
            risk_tier TEXT NOT NULL,
            updated_at TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE merchants (
            merchant_id TEXT PRIMARY KEY,
            risk_tier TEXT NOT NULL,
            updated_at TIMESTAMP
        )
        """
    )

    cursor.executemany(
        "INSERT INTO customer_profiles VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        [
            ("cust_0000001", "ACTIVE", "TRUSTED"),
            ("cust_0000002", "ACTIVE", "WATCHLIST"),
            ("cust_0000003", "SUSPENDED", "BLOCKED"),
            ("cust_0000004", "ACTIVE", "TRUSTED"),
        ],
    )
    cursor.executemany(
        "INSERT INTO merchants VALUES (?, ?, CURRENT_TIMESTAMP)",
        [
            ("merch_001", "LOW"),
            ("merch_002", "HIGH"),
        ],
    )
    conn.commit()
    conn.close()

    return db_url, db_file


def test_sync_operational_risk_feedback_execution_and_idempotency(sqlite_test_db):
    db_url, db_file = sqlite_test_db

    # Mock Spark session returning Gold tables
    mock_spark = MagicMock()
    mock_scored = MagicMock()
    mock_monitoring = MagicMock()

    # Block cust_0000001 (active) and cust_0000003 (already suspended)
    mock_scored.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
        ("cust_0000001",),
        ("cust_0000003",),
    ]
    # Flag merch_001 (low) and merch_002 (already high)
    mock_monitoring.filter.return_value.select.return_value.distinct.return_value.collect.return_value = [
        ("merch_001",),
        ("merch_002",),
    ]

    def mock_table(table_name):
        if "scored_transactions" in table_name:
            return mock_scored
        return mock_monitoring

    mock_spark.table.side_effect = mock_table
    config = LakehouseConfig()

    # First run: updates cust_0000001 and merch_001
    res1 = sync_operational_risk_feedback(
        spark=mock_spark,
        config=config,
        connection_url=db_url,
    )
    assert res1["blocked_customers_identified"] == 2
    assert res1["flagged_merchants_identified"] == 2
    assert res1["customers_suspended"] == 1
    assert res1["merchants_flagged"] == 1

    # Verify state in DB
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT account_status, risk_tier FROM customer_profiles WHERE customer_id = 'cust_0000001'")
    assert cursor.fetchone() == ("SUSPENDED", "BLOCKED")
    cursor.execute("SELECT risk_tier FROM merchants WHERE merchant_id = 'merch_001'")
    assert cursor.fetchone() == ("HIGH",)
    conn.close()

    # Second run with same inputs: 0 rows modified (idempotent)
    res2 = sync_operational_risk_feedback(
        spark=mock_spark,
        config=config,
        connection_url=db_url,
    )
    assert res2["customers_suspended"] == 0
    assert res2["merchants_flagged"] == 0
