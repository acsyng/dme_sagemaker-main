import logging as log
import pandas as pd
import polars as pl
import psycopg2 as dbdriver

from tenacity import retry, wait_fixed, stop_after_attempt

from libs.helper.parameters_helper import ParametersHelper
from libs.logger.cloudwatch_logger import CloudWatchLogger


class DenodoConnection:
    """
    Class for loading data from Denodo and saving to csv files.
    """

    def __init__(self):
        """
        Constructor.
        :param out_dir:
        """
        self.__denodo_con = None
        ph = ParametersHelper("denodo")
        self.__connection_string = "user=%s password=%s host=%s dbname=%s port=%s" % (
            ph.get_parameter("username"),
            ph.get_parameter("password", True),
            ph.get_parameter("server"),
            ph.get_parameter("database"),
            ph.get_parameter("odbc_port"),
        )
        self.__logger = CloudWatchLogger.get_logger()

    def __enter__(self):
        """
        Connect to Denodo server.
        """
        self.__denodo_con = dbdriver.connect(self.__connection_string)
        self.__logger.info("Connection to Denodo has been established")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close Denodo connection.
        :return:
        """
        self.__denodo_con.close()
        self.__logger.info("Denodo connection has been closed")

    @retry(wait=wait_fixed(30), stop=stop_after_attempt(10))
    def get_data(self, sql, package="pandas", schema_overrides=None):
        """
        Get data from SQL query to Denodo.
        :param sql: Denodo query.
        :return: Pandas dataframe.
        """
        self.__logger.info(sql)
        if package == "polars":
            df = pl.read_database(
                query=sql,
                connection=self.__denodo_con,
                schema_overrides=schema_overrides,
            )
        else:  # package == "pandas"
            df = pd.read_sql_query(sql, self.__denodo_con)

        return df

    def save_to_csv(self, sql, csv_name):
        """
        Get data from SQL query to Denodo.
        :param sql: Denodo query.
        :param csv_name: Output csv file name.
        :return: Pandas dataframe.
        """
        self.__logger.info(sql)
        chunks = pd.read_sql_query(sql, self.__denodo_con, chunksize=100000)
        header = True
        for chunk in chunks:
            chunk.to_csv(csv_name, mode="a", index=False, header=header)
            header = False
