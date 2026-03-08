import logging
from pipelines import StockPipeline
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
password = os.getenv("DB_PASSWORD")

# 1. Configuration (In production, these would be in environment variables)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": password,
    "host": "localhost",
    "port": "5432"
}

S3_BUCKET = "ardit-stock-data-lake"
TICKERS_TO_TRACK = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

# 2. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_parquet_path(base_dir: str = "data/silver") -> str:
    """
    Generates a path like: data/silver/stocks_2024_03_08.parquet
    """
    # 1. Ensure the directory exists
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
        
    # 2. Get current date in YYYY_MM_DD format
    today = datetime.now().strftime("%Y_%m_%d")
    
    # 3. Construct filename
    filename = f"stocks_{today}.parquet"
    return os.path.join(base_dir, filename)

def run_market_pipeline():
    """
    Orchestrates the Market Data Pipeline.
    """
    logging.info("--- Starting Market Data Pipeline ---")
    
    # Initialize the specific pipeline
    pipeline = StockPipeline(source_name="YahooFinance", db_config=DB_CONFIG)
    
    try:
        # Step 1: Extract
        raw_data = pipeline.extract(TICKERS_TO_TRACK)
        
        # Step 2: Transform
        clean_data = pipeline.transform(raw_data)
        
        # Step 3: Load
        if clean_data:
            pipeline.load(clean_data)
            logging.info("--- Pipeline Completed Successfully ---")
        else:
            logging.warning("No clean data found to load.")

        local_parquet_path = pipeline.load_to_lake(clean_data, zone="silver")

        # Upload to Mock Cloud Storage S3
        if local_parquet_path:
            pipeline.upload_to_s3(local_parquet_path, S3_BUCKET)
        
        logging.info("--- All Pipeline Stages Completed Successfully ---")

    except Exception as e:
        logging.error(f"Pipeline failed at the orchestration level: {e}")

if __name__ == "__main__":
    run_market_pipeline()