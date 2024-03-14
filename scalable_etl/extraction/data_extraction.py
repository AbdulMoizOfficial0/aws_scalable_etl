import csv
from abc import ABC, abstractmethod
from kafka import KafkaProducer
import requests
import json
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


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
    file_path = config['DEFAULT']['file_path']
    csv_data_source = CSVDataSource(file_path)
    data = csv_data_source.extract()

    kafka_bootstrap_servers = config['DEFAULT']['kafka_bootstrap_servers']
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    api_url = config['DEFAULT']['api_url']
    api_data_source = APIDataSource(api_url)

    api_data = api_data_source.extract()

    for item in api_data:
        topic = config['DEFAULT']['api_topic']
        kafka_producer.send(topic, json.dumps(item).encode('utf-8'))
        kafka_producer.flush()
        kafka_producer.close()
    print("Data Sent to Topic")
