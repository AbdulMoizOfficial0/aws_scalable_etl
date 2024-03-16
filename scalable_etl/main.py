from data_extraction.csv_extractor import CSVExtractor
from data_extraction.mysql_extractor import MySQLExtractor
from data_transformation.mysql_transformer import MySQLTransformation
from data_transformation.csv_transformer import CSVTransformation
from config.config import ETLConfig


def main():
    csv_extractor = CSVExtractor(ETLConfig.CSV_DATA_PATH)
    mysql_extractor = MySQLExtractor(
        ETLConfig.MYSQL_DB,
        ETLConfig.MYSQL_HOST,
        ETLConfig.MYSQL_USER,
        ETLConfig.MYSQL_PASSWORD
    )

    csv_data = csv_extractor.extract_data()
    mysql_data = mysql_extractor.extract_data()

    csv_transformed_data = CSVTransformation.transform(csv_data)
    mysql_transformed_data = MySQLTransformation.transform_data(mysql_data)

    return csv_transformed_data, mysql_transformed_data


if __name__ == "__main__":
    main()
