import random
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text

from libs.helper.parameters_helper import ParametersHelper
from libs.logger.cloudwatch_logger import CloudWatchLogger


class PostgresConnection:
    """
    Class for writing data to Aurora/Postgres db.
    """

    def __init__(self):
        """
        Constructor.
        """
        ph = ParametersHelper("postgres")
        self.username = ph.get_parameter("username")
        self.password = ph.get_parameter("password", True)
        self.server = ph.get_parameter("server")
        self.database = ph.get_parameter("database")
        self.odbc_port = ph.get_parameter("odbc_port")
        self.__logger = CloudWatchLogger.get_logger()

    def __enter__(self):
        """
        Connect to PostgreSQL db.
        """
        self.__pgsql_con = self.get_engine().connect()
        self.__logger.info("Connection to PostgreSQL has been established")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close PostgreSQL connection.
        :return:
        """
        self.__pgsql_con.close()
        self.__logger.info("PostgreSQL connection has been closed")

    def write_pandas_df(self, df, table_name, schema_name="public"):
        df.to_sql(
            name=table_name,
            con=self.get_engine(),
            schema=schema_name,
            if_exists="replace",
            index=False,
        )

    def write_polars_df(self, df, table_name, schema_name="public"):
        for frame in df.iter_slices(n_rows=100000):
            frame.write_database(
                table_name=schema_name + "." + table_name,
                connection=self.get_engine(),
                if_table_exists="append",
                engine_options={"method": "multi"},
            )

        # df.to_pandas().to_sql(
        #     name=table_name,
        #     con=self.get_engine(),
        #     schema=schema_name,
        #     if_exists="replace",
        #     index=False,
        #     chunksize=100000,
        #     method="multi",
        # )

    def run_sql(self, sql):
        self.execute_sql_batch([sql])

    def execute_sql_batch(self, sql_statements):
        """
        Executes a series of sql_statements in order within a single DB connection and transaction.
        This will not commit the changes until all changes have completed.
        :param sql_statements: An iterable of DML SQL statements that are to
        be executed within a single transaction
        :return:
        """
        with self.get_engine().connect() as connection:
            with connection.begin():
                # commit only after all sql_statements complete.  This allows a series of
                # statements to run in one transaction, so we can do things like
                # delete rows, then insert, then clean up in a single connection and transaction
                for sql in sql_statements:
                    self.__logger.info(sql)
                    connection.execute(text(sql))

    def upsert(self, sql, table_name, df, is_spark=True):
        temp_table_name = self.get_temp_table(table_name)
        self.__logger.info(f"Write dataframe to {temp_table_name}")
        if is_spark:
            write_fun = self.write_spark_df
        else:
            write_fun = self.write_pandas_df
        write_fun(df, temp_table_name)
        self.run_sql(sql.format(temp_table_name))
        self.run_sql(f"DROP TABLE {temp_table_name}")

    @staticmethod
    def get_temp_table(table_name):
        suffix = "".join(random.choices("0123456789", k=10))
        return f"{table_name}_{suffix}"

    def write_spark_df(self, spark_df, table_name, table_schema="public"):
        jdbc_url = f"jdbc:postgresql://{self.server}:{self.odbc_port}/{self.database}"
        properties = {
            "user": self.username,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        spark_df.write.jdbc(
            url=jdbc_url,
            table=f"{table_schema}.{table_name}",
            mode="overwrite",
            properties=properties,
        )

    def get_engine(self):
        return create_engine(
            f"postgresql://{self.username}:{self.password}@{self.server}:{self.odbc_port}/{self.database}"
        )

    def get_data(self, sql, package="pandas"):
        """
        Get data from SQL query to postgres connection.
        :param sql: query.
        :param package: pandas or polars.
        :return: Pandas or Polars dataframe.
        """
        with self.get_engine().connect() as conn:
            if package == "polars":
                df = pl.read_database(query=text(sql), connection=conn)
            else:
                df = pd.read_sql(text(sql), conn)

        return df
