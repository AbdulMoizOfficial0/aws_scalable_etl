import csv
import os


class CSVExtractor:
    def __init__(self, csv_data_path):
        self.csv_data_path = csv_data_path

    def extract_data(self):
        data = []
        for file_name in os.listdir(self.csv_data_path):
            if file_name.endswith(".csv"):
                with open(os.path.join(self.csv_data_path, file_name), 'r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        data.append(row)
        return data