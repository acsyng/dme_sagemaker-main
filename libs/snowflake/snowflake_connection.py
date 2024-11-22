import logging as log
import pandas as pd
import polars as pl
import snowflake.connector
import json
import requests

from tenacity import retry, wait_fixed, stop_after_attempt

from libs.helper.parameters_helper import ParametersHelper


class SnowflakeConnection:
    """
    Class for loading data from Denodo and saving to csv files.
    """

    def __init__(self):
        """
        Constructor.
        :param out_dir:
        """
        self.__snowflake_conn = None
        self.__token = None
        ph = ParametersHelper("snowflake")

        self.__username = ph.get_parameter("username")
        self.__password = ph.get_parameter("password", encryption=True)
        self.__account = ph.get_parameter("account")
        self.__database = ph.get_parameter("database")
        self.__warehouse = ph.get_parameter("warehouse")
        self.__schema = ph.get_parameter("schema")
        self.__role = ph.get_parameter("role")
        self.__url = ph.get_parameter("url")

    @retry(wait=wait_fixed(120), stop=stop_after_attempt(3)) # did not optimize these times and retry attempts
    def get_token(self):
        headers = {"Content-Type": "application/json"}
        body = json.dumps({"username": self.__username, "password": self.__password})
        response = requests.post(self.__url, headers=headers, data=body)
        token = response.json().get("token")
        return token

    def __enter__(self):
        """
        Connect to Snowflake server.
        """
        if self.__token is None:
            self.__token = self.get_token()

        self.__snowflake_conn = self.connection_wrapper()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close Snowflake connection.
        :return:
        """
        self.__snowflake_conn.close()

    def connection_wrapper(self):
        """
        return a snowflake connector
        """
        return snowflake.connector.connect(
            user=self.__username,
            account=self.__account,
            authenticator="oauth",
            database=self.__database,
            warehouse=self.__warehouse,
            schema=self.__schema,
            role=self.__role,
            token=self.__token,
        )

    @retry(wait=wait_fixed(30), stop=stop_after_attempt(10))
    def get_data(self, sql, package="pandas", schema_overrides=None, do_lower=True):
        """
        Get data from SQL query to Snowflake.
        :param sql: Snowflake query.
        :return: Pandas dataframe.
        """
        # check if token has expired. This is likely unnecessary?
        if self.__snowflake_conn.expired:
            self.__snowflake_conn.close()  # close current connection
            self.__token = self.get_token()  # update token
            self.__snowflake_conn = (
                self.connection_wrapper()
            )  # make new connection with new token

        if package == "polars":
            df = pl.read_database(
                query=sql,
                connection=self.__snowflake_conn,
                schema_overrides=schema_overrides,
            )
            if do_lower:
                df.columns = [col.lower() for col in df.columns]

        else:
            df = pd.read_sql(sql, self.__snowflake_conn)

        # snowflake has capitalized columns, make lower case to be backwards compatible
        if do_lower:
            df.columns = [col.lower() for col in df.columns]

        return df

    def save_to_csv(self, sql, csv_name):
        """
        Get data from SQL query to Snowflake.
        :param sql: Snowflake query.
        :param csv_name: Output csv file name.
        :return: Pandas dataframe.
        """
        chunks = pd.read_sql_query(sql, self.__snowflake_conn, chunksize=100000)
        header = True
        for chunk in chunks:
            chunk.to_csv(csv_name, mode="a", index=False, header=header)
            header = False
