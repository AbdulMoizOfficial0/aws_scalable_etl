import os
import json
from kafka import KafkaProducer, KafkaConsumer
import configparser

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))


class APITransformation:
    def __init__(self):
        self.consumer = KafkaConsumer(
            config["DEFAULT"]["api_topic"],
            bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"]
        )
        self.producer = KafkaProducer(bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"])

    def transform_data(self, data):
        if "results" not in data or not data["results"]:
            return None

        result = data["results"][0]
        transformed_data = {
            "first_name": result["name"]["first"],
            "last_name": result["name"]["last"],
            "email": result["email"],
            "gender": result["gender"]
        }
        return transformed_data

    def process_data(self):
        for message in self.consumer:
            data = json.loads(message.value.decode('utf-8'))
            transformed_data = self.transform_data(data)
            output_topic = config["DEFAULT"]["api_output_topic"]
            self.producer.send(output_topic, value=json.dumps(transformed_data).encode('utf-8'))
            self.producer.flush()


if __name__ == "__main__":
    api_transformer = APITransformation()
    api_transformer.process_data()
