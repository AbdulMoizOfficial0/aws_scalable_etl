import yaml
import json
import csv
import os
from kafka import KafkaConsumer


def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)



def write_to_csv(data, csv_directory):
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    file_path = os.path.join(csv_directory, 'api_data.csv')
    file_exists = os.path.isfile(file_path)

    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["first", "last", "email", "phone", "dob"])

        writer.writerow([
            data.get('first', ''),
            data.get('last', ''),
            data.get('email', ''),
            data.get('phone', ''),
            data.get('dob', '')
        ])


def consume_messages(consumer, csv_directory):
    for message in consumer:
        try:
            raw_message = message.value.decode('utf-8')
            if raw_message:
                data = json.loads(raw_message)
                print("Received data:", data)  # Print received data for debugging

                if isinstance(data, dict):
                    write_to_csv(data, csv_directory)
                else:
                    print("Received data is not in the expected format")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e} for message: {raw_message}")


if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_directory, '..', 'config', 'config.yaml')

    config = load_config(config_path)
    topic = 'testing'
    csv_directory = config['apiData']

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='api_group'
    )
    consume_messages(consumer, csv_directory)
