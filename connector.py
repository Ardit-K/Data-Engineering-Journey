from typing import List, Dict, Optional
import random
import logging
import math
import psycopg2 # The Postgres driver


# Set up basic logging
logging.basicConfig(level=logging.INFO)

class DataConnector:
    """
    A base class to simulate fetching data from an external source.
    """
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.is_connected = False

    def connect(self) -> bool:
        """Simulates connecting to a source."""
        # Logic: 90% chance of success, 10% chance of failure
        if random.random() > 0.1:
            self.is_connected = True
            logging.info(f"Successfully connected to {self.source_name}")
            return True
        logging.error(f"Failed to connect to {self.source_name}")
        return False

    def fetch_raw_data(self) -> List[Dict]:
        """Returns a list of raw dictionaries simulating user events."""
        if not self.is_connected:
            raise ConnectionError("Must connect before fetching data!")
        
        return [
            {"user_id": 1, "email": "ARDIT@GMAIL.COM", "spent": "150.50"},
            {"user_id": 2, "email": "test@test.com", "spent": None}, # Missing data!
            {"user_id": 3, "email": "QUANT@DEV.IO", "spent": "2000.00"},
        ]

class UserDataProcessor(DataConnector):
    """
    A child class that handles the cleaning of the fetched data.
    """
    def clean_data(self, raw_data: List[Dict]) -> List[Dict]:
        """
        Refined cleaning logic for financial data:
        1. Rounds prices to 2 decimal places.
        2. Filters out records with invalid (NaN or zero) prices.
        3. Ensures volume is an integer.
        """
        clean_results = []
        
        for record in raw_data:
            try:
                # 1. Price Validation & Precision
                close_price = record.get('close')
                
                # Check for NaN or None (Common in financial APIs)
                if close_price is None or math.isnan(close_price) or close_price <= 0:
                    logging.warning(f"Skipping invalid price for {record['ticker']} on {record['date']}: {close_price}")
                    continue
                    
                # Round to 2 decimals for financial consistency
                rounded_price = round(float(close_price), 2)

                # 2. Volume Validation
                volume = int(record.get('volume', 0))

                # 3. Construct the cleaned record
                cleaned_record = {
                    "ticker": record['ticker'],
                    "company_name": record['company_name'],
                    "sector": record['sector'],
                    "date": record['date'],
                    "close": rounded_price,
                    "volume": volume
                }
                
                clean_results.append(cleaned_record)

            except (ValueError, TypeError) as e:
                logging.error(f"Data type error for {record.get('ticker')}: {e}")
                continue

        logging.info(f"Successfully cleaned {len(clean_results)} out of {len(raw_data)} records.")
        return clean_results
    
    def load_to_postgres(self, cleaned_data: List[Dict]):
        """
        Loads data into a Star Schema (dim_users and fact_spending).
        """
        try:
            conn = psycopg2.connect(
                dbname="postgres", user="postgres", password="password", host="localhost", port="5432"
            )
            cur = conn.cursor()

            # 1. Ensure Tables Exist (The DDL phase)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_users (
                    user_id INT PRIMARY KEY,
                    email TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS fact_spending (
                    fact_id SERIAL PRIMARY KEY,
                    user_id INT REFERENCES dim_users(user_id),
                    amount_spent FLOAT,
                    transaction_date DATE DEFAULT CURRENT_DATE
                );
            """)

            # 2. The "Upsert" and "Insert" Logic
            for row in cleaned_data:
                # Update Dimension: If user exists, update email. If not, insert.
                cur.execute("""
                    INSERT INTO dim_users (user_id, email) 
                    VALUES (%s, %s) 
                    ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email;
                """, (row['user_id'], row['email']))

                # Insert Fact: Record the actual spending event
                cur.execute("""
                    INSERT INTO fact_spending (user_id, amount_spent) 
                    VALUES (%s, %s);
                """, (row['user_id'], row['spent']))
            
            conn.commit()
            logging.info("Star Schema load complete.")
            
            cur.close()
            conn.close()

        except Exception as e:
            logging.error(f"Database error: {e}")
            if conn: conn.rollback() # Expert tip: Roll back on error!


# --- EXECUTION ---
# --- EXECUTION ---
if __name__ == "__main__":
    processor = UserDataProcessor("E-commerce_API")
    
    if processor.connect():
        raw_data = processor.fetch_raw_data()
        cleaned = processor.clean_data(raw_data)
            
        # This completes the pipeline!
        processor.load_to_postgres(cleaned)