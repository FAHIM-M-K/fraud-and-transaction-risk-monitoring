from unittest.mock import MagicMock
from payment_risk.common.config import LakehouseConfig
from payment_risk.gold.azure_sql_sink import (
    get_azure_sql_connection_properties,
    write_table_to_azure_sql,
    publish_gold_mart_to_azure_sql,
)


def test_get_azure_sql_connection_properties():
    props = get_azure_sql_connection_properties("sqladmin", "Secr3t!", batch_size=5000)
    assert props["user"] == "sqladmin"
    assert props["password"] == "Secr3t!"
    assert props["driver"] == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert props["batchsize"] == "5000"
    assert props["isolationLevel"] == "READ_UNCOMMITTED"
    assert props["tableLock"] == "true"


def test_write_table_to_azure_sql():
    mock_df = MagicMock()
    mock_df.count.return_value = 150
    mock_writer = MagicMock()
    mock_df.write.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value = mock_writer
    mock_writer.option.return_value.mode.return_value = mock_writer

    props = get_azure_sql_connection_properties("user", "pass")
    count = write_table_to_azure_sql(
        df=mock_df,
        jdbc_url="jdbc:sqlserver://host:1433;database=test;",
        table_name="dbo.dim_merchants",
        connection_properties=props,
        mode="overwrite",
    )
    assert count == 150
    mock_writer.save.assert_called_once()


def test_publish_gold_mart_to_azure_sql():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 42
    mock_spark.table.return_value = mock_df

    mock_writer = MagicMock()
    mock_df.write.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value = mock_writer
    mock_writer.option.return_value.mode.return_value = mock_writer

    config = LakehouseConfig(catalog_name="payment_risk")
    results = publish_gold_mart_to_azure_sql(
        spark=mock_spark,
        config=config,
        server="sql-server.database.windows.net",
        database="payment_risk_db",
        username="admin",
        password="pwd",
    )
    assert len(results) == 6
    assert results["dbo.dim_merchants"] == 42
    assert results["dbo.fact_scored_transactions"] == 42
