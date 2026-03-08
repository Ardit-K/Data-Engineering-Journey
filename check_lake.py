import pandas as pd
import boto3
from io import BytesIO



# Point to LocalStack
s3 = boto3.client('s3', endpoint_url="http://localhost:4566",
                aws_access_key_id="test",
                aws_secret_access_key="test",
                region_name="us-east-1")

# 1. Download the file into memory
bucket = "ardit-stock-data-lake"
key = "stocks_2026_03_08.parquet" # Update to your actual filename

obj = s3.get_object(Bucket=bucket, Key=key)

# 2. Read the Parquet bytes directly into a Pandas DataFrame
df = pd.read_parquet(BytesIO(obj['Body'].read()))

print("--- Data Lake Sample ---")
print(df.head())
print(f"\nTotal Rows in Lake: {len(df)}")