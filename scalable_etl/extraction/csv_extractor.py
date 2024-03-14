import os
import csv
from kafka import KafkaProducer
import configparser
import json


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))


class CSVExtractor:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"])

    def extract(self):
        csv_folder = config["DEFAULT"]["file_path"]
        csv_topic = config["DEFAULT"]["csv_topic"]

        for file_name in os.listdir(csv_folder):
            if file_name.endswith(".csv"):
                file_path = os.path.join(csv_folder, file_name)
                self.read_csv_file(file_path, csv_topic)

    def read_csv_file(self, file_path, topic):
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                self.send_to_kafka(topic, row)

    def send_to_kafka(self, topic, data):
        data = json.dumps(data).encode('utf-8')
        self.producer.send(topic, value=data)
        self.producer.flush()


if __name__ == "__main__":
    extractor = CSVExtractor()
    extractor.extract()