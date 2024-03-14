import csv
from abc import ABC, abstractmethod
from kafka import KafkaProducer
import requests
import json


class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass


class CSVDataSource(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        data = []
        with open(self.file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data.append(row)
        return data

class APIDataSource(Extractor):
    def __init__(self, url):
        self.url = url

    def extract(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            return []



if __name__ == "__main__":
    file_path = '../data/BTC-USD.csv'
    csv_data_source = CSVDataSource(file_path)
    data = csv_data_source.extract()


    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
    api_data_source = APIDataSource('https://query1.finance.yahoo.com/v7/finance/download/BTC-USD?period1=1678793548&period2=1710415948&interval=1d&events=history&includeAdjustedClose=true')
    api_data = api_data_source.extract()

    for item in api_data:
        kafka_producer.send('api_data', json.dumps(item).encode('utf-8'))
        kafka_producer.flush()
        kafka_producer.close()
    print("Data Sent to Topic")