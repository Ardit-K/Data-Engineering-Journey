import yfinance as yf
import logging
import math
import psycopg2
from typing import List, Dict
from connector import DataConnector  # Import your base class

class StockPipeline(DataConnector):
    """
    Final Exam Pipeline: Fetches, cleans, and loads stock data.
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
        """Applies the cleaning logic we discussed."""
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