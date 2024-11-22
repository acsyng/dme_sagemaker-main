import polars as pl
from unittest.mock import patch
from libs.postgres.postgres_connection import PostgresConnection


@patch("libs.postgres.postgres_connection.pd")
@patch("libs.postgres.postgres_connection.create_engine")
@patch("libs.postgres.postgres_connection.CloudWatchLogger")
@patch("libs.postgres.postgres_connection.ParametersHelper")
def test_postgres_connection(param_helper_mock, log_mock, engine_mock, pd_mock):
    pc = PostgresConnection()
    dummy_sql = "select * from some_table"
    pc.get_data(dummy_sql)
    engine_mock.assert_called_once()
    pd_mock.read_sql.assert_called_once()
