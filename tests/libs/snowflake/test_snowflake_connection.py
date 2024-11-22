from unittest.mock import patch
from libs.snowflake.snowflake_connection import SnowflakeConnection

import pytest


@pytest.fixture()
@patch("libs.snowflake.snowflake_connection.pd.read_sql_query")
@patch("libs.snowflake.snowflake_connection.snowflake.connector")
@patch("libs.snowflake.snowflake_connection.ParametersHelper")
def test_snowflake_connection(param_helper_mock, dbdriver_mock, pd_mock):
    param_helper_mock.get_parameter.return_value = ""
    with SnowflakeConnection() as sc:
        dummy_sql = "select * from some_table"
        sc.get_data(dummy_sql)
        pd_mock.assert_called_with(dummy_sql, sc._SnowflakeConnection__snowflake_conn)
