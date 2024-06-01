class CSVTransformer:
    def transform(self, data_frames):
        # Example transformation logic for CSV data
        transformed_data = []
        for df in data_frames:
            df['new_column'] = df['existing_column'] * 2  # Example transformation
            transformed_data.append(df)
        return transformed_data



