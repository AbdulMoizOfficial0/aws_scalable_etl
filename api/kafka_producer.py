import yaml
import requests
import json
import time
from kafka import KafkaProducer
import os

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json().get('results', [])
    return []

def produce_messages(producer, topic, api_url):
    while True:
        data = fetch_data_from_api(api_url)
        if data:
            for item in data[:5]:  # Pick any 5 records
                message = json.dumps(item).encode('utf-8')
                producer.send(topic, message)
        time.sleep(3)  # Fetch data every 60 seconds

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_directory, '..', 'config', 'config.yaml')

    config = load_config(config_path)
    api_url = config['API']
    topic = 'testing'

    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    produce_messages(producer, topic, api_url)
