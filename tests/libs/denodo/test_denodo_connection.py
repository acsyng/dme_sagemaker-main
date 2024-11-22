from unittest.mock import patch
from libs.denodo.denodo_connection import DenodoConnection


@patch("libs.denodo.denodo_connection.CloudWatchLogger")
@patch("libs.denodo.denodo_connection.pd.read_sql_query")
@patch("libs.denodo.denodo_connection.dbdriver")
@patch("libs.denodo.denodo_connection.ParametersHelper")
def test_denodo_connection(param_helper_mock, dbdriver_mock, pd_mock, log_mock):
    param_helper_mock.get_parameter.retrun_value = ""
    with DenodoConnection() as dc:
        dummy_sql = "select * from some_table"
        dc.get_data(dummy_sql)
        pd_mock.assert_called_with(dummy_sql, dc._DenodoConnection__denodo_con)
