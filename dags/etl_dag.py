from airflow import DAG
import pandas as pd
import sys
from airflow.operators.python import PythonOperator
from datetime import datetime
import yaml

sys.path.append('/opt/airflow/data_extraction/')
sys.path.append('/opt/airflow/data_loading/')
sys.path.append('/opt/airflow/data_transformation/')
sys.path.append('/opt/airflow/config/')

from csv_extractor import CSVExtractor
from mssql_extractor import MSSQLExtractor
from csv_transformer import CSVTransformer
from mssql_transformer import MSSQLTransformer
from data_loading import S3Loader


# Load configurations
with open('/opt/airflow/config/config.yaml') as config_file:
    config = yaml.safe_load(config_file)


def extract_csv_data():
    csv_extractor = CSVExtractor(config_file['csv_directory'])
    return csv_extractor.extract()


def extract_mssql_data():
    mssql_extractor = MSSQLExtractor(config_file['mssql_connection_string'])
    query = "SELECT * FROM ETC-USD"  # Define your query
    return mssql_extractor.extract(query)


def transform_csv_data(**context):
    csv_transformer = CSVTransformer()
    extracted_data = context['ti'].xcom_pull(task_ids='extract_csv_data')
    return csv_transformer.transform(extracted_data)


def transform_mssql_data(**context):
    mssql_transformer = MSSQLTransformer()
    extracted_data = context['ti'].xcom_pull(task_ids='extract_mssql_data')
    return mssql_transformer.transform(extracted_data)


def load_data_to_s3(**context):
    csv_data = context['ti'].xcom_pull(task_ids='transform_csv_data')
    mssql_data = context['ti'].xcom_pull(task_ids='transform_mssql_data')

    combined_data = pd.concat(csv_data + [mssql_data])

    s3_loader = S3Loader(
        aws_access_key=config['aws']['access_key'],
        aws_secret_key=config['aws']['secret_key'],
        bucket_name=config['aws']['bucket_name']
    )
    s3_loader.load(combined_data, 'combined_data.csv')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('etl_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    extract_csv_task = PythonOperator(
        task_id='extract_csv_data',
        python_callable=extract_csv_data
    )

    extract_mssql_task = PythonOperator(
        task_id='extract_mssql_data',
        python_callable=extract_mssql_data
    )

    transform_csv_task = PythonOperator(
        task_id='transform_csv_data',
        python_callable=transform_csv_data,
        provide_context=True
    )

    transform_mssql_task = PythonOperator(
        task_id='transform_mssql_data',
        python_callable=transform_mssql_data,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_data_to_s3,
        provide_context=True
    )

    extract_csv_task >> transform_csv_task
    extract_mssql_task >> transform_mssql_task
    [transform_csv_task, transform_mssql_task] >> load_data_task
