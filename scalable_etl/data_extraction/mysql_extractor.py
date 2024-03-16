import mysql.connector
from config.config import ETLConfig


class MySQLExtractor:
    def __init__(self):
        self.host = ETLConfig.MYSQL_HOST
        self.user = ETLConfig.MYSQL_USER
        self.password = ETLConfig.MYSQL_PASSWORD
        self.database = ETLConfig.MYSQL_INPUT_DB

    def extract_data(self):
        data = []
        conn = mysql.connector.connect(host=self.host,
                                       user=self.user,
                                       password=self.password,
                                       database=self.database)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM bitcoin')
        for row in cursor.fetchall():
            data.append(row)

        cursor.close()
        conn.close()
        return data

