import os

class ETLConfig:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CSV_DATA_PATH = os.path.join(BASE_DIR, '..', 'csv_data')
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'root'
    MYSQL_INPUT_DB = 'stocks'
    MYSQL_OUTPUT_DB = 'output'
    JSON_DATA_PATH = os.path.join(BASE_DIR, '..', 'json_data')
    MSSQL_SERVER = 'CRIBL-MOIZAABD1'
    MSSQL_DATABASE = 'bookstore'
    MSSQL_USERNAME = 'root'
    MSSQL_PASSWORD = 'root'
