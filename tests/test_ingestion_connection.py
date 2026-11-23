import pytest
from payment_risk.bronze.ingestion import parse_postgres_jdbc_connection


def test_parse_postgres_raw_neon_uri():
    neon_url = "postgresql://neondb_owner:npg_secret123@ep-polished-dawn-12345.us-east-2.aws.neon.tech/neondb?sslmode=require"
    clean_url, props = parse_postgres_jdbc_connection(neon_url)

    assert clean_url == "jdbc:postgresql://ep-polished-dawn-12345.us-east-2.aws.neon.tech/neondb"
    assert props["driver"] == "org.postgresql.Driver"
    assert props["user"] == "neondb_owner"
    assert props["password"] == "npg_secret123"
    assert props["sslmode"] == "require"


def test_parse_postgres_with_jdbc_prefix():
    jdbc_url = "jdbc:postgresql://neondb_owner:npg_secret123@ep-polished-dawn-12345.us-east-2.aws.neon.tech/neondb?sslmode=require"
    clean_url, props = parse_postgres_jdbc_connection(jdbc_url)

    assert clean_url == "jdbc:postgresql://ep-polished-dawn-12345.us-east-2.aws.neon.tech/neondb"
    assert props["user"] == "neondb_owner"
    assert props["password"] == "npg_secret123"
    assert props["sslmode"] == "require"


def test_parse_postgres_standard_jdbc():
    standard_url = "jdbc:postgresql://dbhost.com:5432/analytics_db?user=admin&password=pass&sslmode=require"
    clean_url, props = parse_postgres_jdbc_connection(standard_url)

    assert clean_url == "jdbc:postgresql://dbhost.com:5432/analytics_db"
    assert props["user"] == "admin"
    assert props["password"] == "pass"
    assert props["sslmode"] == "require"
