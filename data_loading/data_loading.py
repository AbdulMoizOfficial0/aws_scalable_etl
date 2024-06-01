import boto3
import pandas as pd
from io import StringIO

class S3Loader:
    def __init__(self, aws_access_key, aws_secret_key, bucket_name):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        self.bucket_name = bucket_name

    def load(self, combined_data, s3_key):
        csv_buffer = StringIO()
        combined_data.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
