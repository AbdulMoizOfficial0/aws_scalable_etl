import os
import pandas as pd

class CSVExtractor:
    def __init__(self, csv_directory):
        self.csv_directory = csv_directory

    def extract(self):
        data_frames = []
        for file in os.listdir(self.csv_directory):
            if file.endswith('.csv'):
                file_path = os.path.join(self.csv_directory, file)
                df = pd.read_csv(file_path)
                data_frames.append(df)
        return data_frames
