import json
import os

class JSONExtractor:
    def __init__(self,json_data_path):
        self.json_data_path = json_data_path

    def extract_data(self):
        data = []
        for file_name in os.listdir(self.json_data_path):
            if file_name.endswith(".json"):
                with open(os.path.join(self.json_data_path, file_name), 'r') as file:
                    json_data = json.load(file)
                    data.extend(json_data)
        return data