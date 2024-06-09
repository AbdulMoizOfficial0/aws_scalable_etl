class MSSQLTransformer:
    def transform(self, data_frame):
        data_frame['new_column'] = data_frame['existing_column'] + 10
        return data_frame
