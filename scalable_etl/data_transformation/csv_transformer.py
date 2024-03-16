class CSVTransformation:
    @staticmethod
    def transform(data):
        header = data[0]
        adj_close_index = header.index('Adj Close')

        transformed_data = []
        for row in data[1]:
            transformed_row = row[:adj_close_index] + row[adj_close_index + 1]
            transformed_data.append(transformed_row)
        return [header[:adj_close_index] + header[adj_close_index + 1:]] + transformed_data