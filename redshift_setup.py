import os
import psycopg2
from dotenv import load_dotenv

# Load the Redshift credentials and S3 bucket from .env
load_dotenv()

def setup_redshift_warehouse():
    host = os.getenv("REDSHIFT_HOST")
    port = os.getenv("REDSHIFT_PORT", "5439")
    db = os.getenv("REDSHIFT_DB", "dev")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    role_arn = os.getenv("REDSHIFT_ROLE_ARN")
    bucket = os.getenv("S3_BUCKET_NAME")

    if not all([host, user, password, role_arn]):
        print("Error: Missing Redshift credentials in .env file!")
        return

    print("Connecting to Redshift Serverless...")
    
    try:
        # 1. Connect to Redshift
        conn = psycopg2.connect(
            host=host, port=port, dbname=db, user=user, password=password
        )
        conn.autocommit = True  # Required for Redshift COPY commands
        cur = conn.cursor()
        print("Successfully connected!")

        # 2. Create the Gold Schema Table
        print("Creating gold_moving_averages table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gold_moving_averages (
                ticker VARCHAR(10),
                avg_close FLOAT,
                avg_volume FLOAT
            );
        """)

        # 3. Truncate (Clear) the table in case we run this multiple times
        cur.execute("TRUNCATE TABLE gold_moving_averages;")

        # 4. The COPY Command (Ingesting from S3)
        s3_path = f"s3://{bucket}/gold/daily_moving_averages/"
        print(f"Running COPY command to ingest Parquet files from {s3_path}...")
        
        copy_query = f"""
            COPY gold_moving_averages
            FROM '{s3_path}'
            IAM_ROLE '{role_arn}'
            FORMAT AS PARQUET;
        """
        cur.execute(copy_query)

        # 5. Verify the Data Landed!
        cur.execute("SELECT * FROM gold_moving_averages;")
        rows = cur.fetchall()
        
        print(f"\nSUCCESS! Found {len(rows)} rows in your Data Warehouse:")
        print("-" * 40)
        for row in rows:
            print(f"Ticker: {row[0]} | Avg Close: ${row[1]} | Avg Vol: {row[2]}")
        print("-" * 40)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"\nRedshift Error: {e}")

if __name__ == "__main__":
    setup_redshift_warehouse()