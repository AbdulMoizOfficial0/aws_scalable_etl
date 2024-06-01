class MSSQLTransformer:
    def transform(self, data_frame):
        # Example transformation logic for MS SQL data
        data_frame['new_column'] = data_frame['existing_column'] + 10  # Example transformation
        return data_frame
