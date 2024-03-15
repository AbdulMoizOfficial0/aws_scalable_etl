import pyodbc

class MsSQLExtractor:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

    def extract_data(self):
        data = []
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={self.server};'
            f'DATABASE={self.database};'
            f'UID={self.username};'
            f'PWD={self.password}'
        )
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM table_name')
        for row in cursor.fetchall():
            data.append(row)

        cursor.close()
        conn.close()
        return data