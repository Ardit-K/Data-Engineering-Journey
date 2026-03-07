import logging
from pipelines import StockPipeline

# 1. Configuration (In production, these would be in environment variables)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

TICKERS_TO_TRACK = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]

# 2. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_market_pipeline():
    """
    Orchestrates the Phase 1 Final Exam Pipeline.
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

    except Exception as e:
        logging.error(f"Pipeline failed at the orchestration level: {e}")

if __name__ == "__main__":
    run_market_pipeline()