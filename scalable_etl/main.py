from data_extraction.csv_extractor import CSVExtractor
from data_extraction.mysql_extractor import MySQLExtractor
from data_transformation.csv_transformer import CSVTransformation
from data_transformation.mysql_transformer import MySQLTransformer
from data_loading.data_loading import MySQLLoader
from config.config import ETLConfig

def main():
    csv_extractor = CSVExtractor(ETLConfig.CSV_DATA_PATH)
    mysql_extractor = MySQLExtractor()

    csv_data = csv_extractor.extract_data()
    mysql_data = mysql_extractor.extract_data()

    csv_transformed_data = CSVTransformation.transform(csv_data)
    mysql_transformed_data = MySQLTransformer.transform_data(mysql_data)

    MySQLLoader.load_data(mysql_transformed_data)

    return csv_transformed_data, mysql_transformed_data

if __name__ == "__main__":
    main()
