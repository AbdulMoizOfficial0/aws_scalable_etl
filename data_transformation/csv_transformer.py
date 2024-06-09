class CSVTransformer:
    def transform(self, data_frames):
        transformed_data = []
        for df in data_frames:
            df['new_column'] = df['existing_column'] * 2
            transformed_data.append(df)
        return transformed_data



