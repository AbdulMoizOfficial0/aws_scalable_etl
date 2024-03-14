from kafka import KafkaProducer, KafkaConsumer
import json
import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

class CSVTransformer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            config["DEFAULT"]["csv_topic"],
            bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"],
            value_deserializer=lambda x: x.decode('utf-8')
        )
        self.producer = KafkaProducer(bootstrap_servers=config["DEFAULT"]["kafka_bootstrap_servers"])

    def transform_data(self, data):
        rows = data.split('\n')

        header = rows[0].split(',')
        new_header = [col for col in header if "Adj Close" not in col]

        new_rows = [','.join(new_header)]
        for row in rows[1:]:
            cols = row.split(',')
            new_cols = [col for idx, col in enumerate(cols) if idx != 5]  # Exclude the 6th column
            new_row = ','.join(new_cols)
            new_rows.append(new_row)

        transformed_data = '\n'.join(new_rows)
        return transformed_data

    def process_data(self):
        for message in self.consumer:
            data = message.value
            transformed_data = self.transform_data(data)
            output_topic = config["DEFAULT"]["csv_output_topic"]
            self.producer.send(output_topic, value=transformed_data.encode('utf-8'))
            self.producer.flush()

if __name__ == "__main__":
    csv_transformer = CSVTransformer()
    csv_transformer.process_data()
