class MySQLTransformer:
    @staticmethod
    def transform_data(data):
        transformed_data = []
        for row in data:
            # Convert tuple to list
            row_list = list(row)
            # Assuming the ticker column index is known
            ticker_index = 6
            # Modify the ticker column value to lowercase
            row_list[ticker_index] = row_list[ticker_index].lower()
            # Convert back to tuple and append to transformed_data
            transformed_data.append(tuple(row_list))
        return transformed_data
