import os
import csv
from abc import ABC, abstractmethod
from kafka import KafkaProducer
import requests
import json
import configparser

# Get the path to the config file
config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')

# Read the config file
config = configparser.ConfigParser()
config.read(config_path)

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class CSVDataSource(Extractor):
    def __init__(self):
        self.file_path = config['DEFAULT']['file_path']
        self.csv_files = [f for f in os.listdir(self.file_path) if f.endswith('.csv')]

    def extract(self):
        data = []
        for csv_file in self.csv_files:
            with open(os.path.join(self.file_path, csv_file), 'r') as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    data.append(row)
            return data

class KafakDataSender:
    def __init__(self, topic):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=config['DEFAULT']['kafka_bootstrap_servers'])

    def send_data(self, data):
        for item in data:
            self.producer.send(self.topic, json.dumps(item).encode('utf-8'))
        self.producer.flush()
        self.producer.close()

class APIDataSource(Extractor):
    def __init__(self):
        self.url = config['DEFAULT']['api_url']

    def extract(self):
        response = requests.get(self.url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            return []

# Usage
if __name__ == "__main__":
    csv_data_source = CSVDataSource()
    csv_data = csv_data_source.extract()
    print("CSV Data:", csv_data)

    kafka_data_sender = KafakDataSender(config['DEFAULT']['csv_topic'])
    kafka_data_sender.send_data(csv_data)

    api_data_source = APIDataSource()
    api_data = api_data_source.extract()
    print("API Data", api_data)

    kafka_data_sender = KafakDataSender(config['DEFAULT']['api_topic'])
    kafka_data_sender.send_data(api_data)

    print("Data sent to API Kafka")