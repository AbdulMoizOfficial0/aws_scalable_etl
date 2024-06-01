import pyodbc
import pandas as pd

class MSSQLExtractor:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def extract(self, query):
        connection = pyodbc.connect(self.connection_string)
        df = pd.read_sql(query, connection)
        connection.close()
        return df
