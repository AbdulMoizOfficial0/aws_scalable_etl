import os
import pandas as pd

class KafkaExtractor:
    def __init__(self, api_data):
        self.api_data = api_data

    def extract(self):
        data_frames = []
        for file in os.listdir(self.api_data):
            if file.endswith('.csv'):
                file_path = os.path.join(self.api_data, file)
                df = pd.read_csv(file_path)
                data_frames.append(df)
        return data_frames
