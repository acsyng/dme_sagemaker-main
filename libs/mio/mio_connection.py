import redshift_connector
from libs.helper.parameters_helper import ParametersHelper

class MioConnection:
    """
    Class for writing data to Aurora/Postgres db.
    """

    def __init__(self):
        """
        Constructor.
        """
        ph = ParametersHelper('mio')
        self.username = ph.get_parameter('username')
        self.password = ph.get_parameter('password', True)
        self.server = ph.get_parameter('server')
        self.database = ph.get_parameter('database')
        self.odbc_port = ph.get_parameter('odbc_port')

    def get_data(self, sql):
        """
        Get data from SQL query to postgres connection.
        :param sql: query.
        :return: Pandas dataframe.
        """

        with redshift_connector.connect(
                host=self.server,
                database=self.database,
                port=self.odbc_port,
                user=self.username,
                password=self.password
            ) as conn:

            # Create a Cursor object
            cursor = conn.cursor()

            # Query a table using the Cursor
            cursor.execute(sql)

            #Retrieve the query result set
            df = cursor.fetch_dataframe()

            # close the cursor
            cursor.close()

        
        return df
