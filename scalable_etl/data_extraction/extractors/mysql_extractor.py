import mysql.connector


class MySQLExtractor:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

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

