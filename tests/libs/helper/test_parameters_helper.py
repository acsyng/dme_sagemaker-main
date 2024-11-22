import os
from unittest.mock import patch

from libs.helper.parameters_helper import ParametersHelper


@patch('libs.helper.parameters_helper.boto3.client')
def test_parameters_helper(ssm_mock):
    os.environ['ENVIRONMENT'] = 'test'
    param_name = 'server'
    ph = ParametersHelper('denodo')
    ph.get_parameter(param_name)
    ssm_mock.return_value.get_parameter.assert_called_once_with(Name=f'/ap/advancement_test/denodo/{param_name}',
                                                                WithDecryption=False)
