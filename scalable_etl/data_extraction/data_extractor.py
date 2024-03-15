from extractors.csv_extractor import CSVExtractor
from extractors.mysql_extractor import MySQLExtractor
from config.config import ETLConfig
from extractors.json_extractor import JSONExtractor
from extractors.mssql_extractor import MsSQLExtractor


class DataExtractor:
    @staticmethod
    def extract_data():
        csv_extractor = CSVExtractor(ETLConfig.CSV_DATA_PATH)
        mysql_extractor = MySQLExtractor(
            ETLConfig.MYSQL_HOST,
            ETLConfig.MYSQL_USER,
            ETLConfig.MYSQL_PASSWORD,
            ETLConfig.MYSQL_DB
        )
        json_extractor = JSONExtractor(ETLConfig.JSON_DATA_PATH)
        mssql_extractor = MsSQLExtractor(
            ETLConfig.MSSQL_SERVER,
            ETLConfig.MSSQL_DATABASE,
            ETLConfig.MSSQL_USERNAME,
            ETLConfig.MSSQL_PASSWORD
        )

        csv_data = csv_extractor.extract_data()
        mysql_data = mysql_extractor.extract_data()
        json_data = json_extractor.extract_data()
        mssql_data = mssql_extractor.extract_data()

        return csv_data, mysql_data, json_data, mssql_data
