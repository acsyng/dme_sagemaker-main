import os

import boto3


class ParametersHelper:
    def __init__(self, section):
        self.__section = section
        self.__ssm = boto3.client('ssm', region_name='us-east-1')

    def get_parameter(self, name, encryption=False):
        """
        Get parameter from Amazon parameter storage.
        :param name: Parameter name
        :param encryption: Encryption flag.
        :return: Parameter value.
        """
        param_name = f'/ap/advancement_{os.getenv("ENVIRONMENT")}/{self.__section}/{name}'
        return self.__ssm.get_parameter(Name=param_name, WithDecryption=encryption)['Parameter']['Value']
