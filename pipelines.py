import yfinance as yf
import logging
import math
import psycopg2
from typing import List, Dict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os
import boto3
import os
from connector import DataConnector  # Import your base class

class StockPipeline(DataConnector):
    """
    Fetches, cleans, and loads stock data.
    """
    def __init__(self, source_name: str, db_config: Dict):
        super().__init__(source_name)
        self.db_config = db_config

    def extract(self, tickers: List[str]) -> List[Dict]:
        """Fetches raw data from Yahoo Finance."""
        raw_data = []
        for ticker in tickers:
            logging.info(f"Extracting {ticker}...")
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d")
            info = stock.info
            
            for date, row in hist.iterrows():
                raw_data.append({
                    "ticker": ticker,
                    "company_name": info.get('longName', 'Unknown'),
                    "sector": info.get('sector', 'Unknown'),
                    "date": date.strftime('%Y-%m-%d'),
                    "close": row['Close'],
                    "volume": row['Volume']
                })
        return raw_data

    def transform(self, raw_data: List[Dict]) -> List[Dict]:
        """Applies the cleaning logic."""
        clean_results = []
        for record in raw_data:
            try:
                close_price = record.get('close')
                if close_price is None or math.isnan(close_price) or close_price <= 0:
                    continue
                
                # Round for precision, ensure types
                record['close'] = round(float(close_price), 2)
                record['volume'] = int(record['volume'])
                clean_results.append(record)
            except Exception as e:
                logging.warning(f"Skipping row for {record.get('ticker')}: {e}")
        return clean_results

    def load(self, clean_data: List[Dict]):
        """Loads data into the Star Schema in Postgres."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            # Create Star Schema Tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_stocks (
                    ticker TEXT PRIMARY KEY,
                    company_name TEXT,
                    sector TEXT
                );
                CREATE TABLE IF NOT EXISTS fact_prices (
                    fact_id SERIAL PRIMARY KEY,
                    ticker TEXT REFERENCES dim_stocks(ticker),
                    price_date DATE,
                    close_price FLOAT,
                    volume BIGINT,
                    UNIQUE(ticker, price_date) -- Prevents duplicates
                );
            """)

            for row in clean_data:
                # Upsert Dimension
                cur.execute("""
                    INSERT INTO dim_stocks (ticker, company_name, sector)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (ticker) DO NOTHING;
                """, (row['ticker'], row['company_name'], row['sector']))

                # Insert Fact
                cur.execute("""
                    INSERT INTO fact_prices (ticker, price_date, close_price, volume)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker, price_date) DO NOTHING;
                """, (row['ticker'], row['date'], row['close'], row['volume']))

            conn.commit()
            cur.close()
            conn.close()
            logging.info("Load complete!")
        except Exception as e:
            logging.error(f"Load failed: {e}")

    def save_as_parquet(self, clean_data: List[Dict], filename: str):
        """
        Converts a list of dicts to a Pandas DataFrame and saves to Parquet.
        """
        # 1. Convert List of Dicts -> Pandas DataFrame
        df = pd.DataFrame(clean_data)
        
        # 2. Ensure types are correct (Very important for Parquet!)
        df['date'] = pd.to_datetime(df['date'])
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)
        
        # 3. Save to Parquet format
        # This creates a file that is highly compressed and optimized for speed
        df.to_parquet(filename, engine='pyarrow', compression='snappy')
        
        print(f"Successfully saved {len(df)} rows to {filename}")

    def load_to_lake(self, clean_data: List[Dict], zone: str = "silver"):
        """
        The 'Modern' Load: Writing to a local Parquet file (The Lake).
        """
        # 1. Construct the path
        today = datetime.now().strftime("%Y_%m_%d")
        base_path = f"data/{zone}"
        os.makedirs(base_path, exist_ok=True)
        full_path = f"{base_path}/stocks_{today}.parquet"
        
        # 2. Convert to DataFrame
        df = pd.DataFrame(clean_data)
        
        # 3. Save as Parquet
        df.to_parquet(full_path, engine='pyarrow', compression='snappy')
        logging.info(f"Phase 2: Data successfully loaded to Lake at {full_path}")
        return full_path  # Return the path for potential upload to S3

    def upload_to_s3(self, local_path: str, bucket_name: str, zone: str = "silver"):
        """
        Uploads your Parquet file to the LocalStack S3 bucket within the correct zone.
        """
        # Create the client pointing to LocalStack
        s3 = boto3.client(
        's3',
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
)

        try:
            # 1. Create bucket if it doesn't exist
            s3.create_bucket(Bucket=bucket_name)
            
            # 2. Extract filename and prepend the zone prefix
            file_name = os.path.basename(local_path)
            s3_key = f"{zone}/{file_name}"  # This creates the virtual 'folder' in S3
            
            # 3. Upload using the full prefixed key
            s3.upload_file(local_path, bucket_name, s3_key)
            logging.info(f"Successfully uploaded to local S3 at s3://{bucket_name}/{s3_key}")
            
        except Exception as e:
            logging.error(f"S3 Upload failed: {e}")