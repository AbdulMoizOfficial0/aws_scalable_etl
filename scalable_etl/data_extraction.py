import csv
from abc import ABC, abstractmethod

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class CSVDataSource(Extractor):
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        data = []
        with open(self.file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data.append(row)
        return data

if __name__ == "__main__":
    file_path = 'scalable_etl/data'
    csv_data_source = CSVDataSource(file_path)
    data = csv_data_source.extract()
    print(data)