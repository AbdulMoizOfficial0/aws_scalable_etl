import mysql.connector
from config.config import ETLConfig


class MySQLLoader:
    @staticmethod
    def load_data(data):
        conn = mysql.connector.connect(
            host=ETLConfig.MYSQL_HOST,
            user=ETLConfig.MYSQL_USER,
            password=ETLConfig.MYSQL_PASSWORD,
            database=ETLConfig.MYSQL_OUTPUT_DB
        )
        cursor = conn.cursor()

        query = ("INSERT INTO output (date_, open_price, highest_price, lowest_price, close_price, volume, "
                 "ticker) VALUES (%s, %s, %s, %s, %s, %s, %s)")
        cursor.executemany(query, data)

        conn.commit()
        cursor.close()
        conn.close()

