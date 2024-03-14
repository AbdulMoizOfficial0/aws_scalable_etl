import requests
from kafka import KafkaProducer
import json
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

class APIExtractor:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"])

    def extract(self):
        api_url = config["DEFAULT"]["api_url"]
        api_topic = config["DEFAULT"]["api_topic"]

        data = self.fetch_data_from_api(api_url)
        self.send_to_kafka(api_topic, data)

    def fetch_data_from_api(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        return None

    def send_to_kafka(self, topic, data):
        data = json.dumps(data).encode('utf-8')
        self.producer.send(topic, value=data)
        self.producer.flush()

if __name__ == "__main__":
    extractor = APIExtractor()
    extractor.extract()