import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round
import logging

# Load the real AWS credentials from the .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_stock_data():
    # Fetch credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not all([aws_access_key, aws_secret_key, bucket_name]):
        logging.error("Missing AWS credentials or bucket name in .env file.")
        return

    logging.info("Initializing Spark Session for REAL AWS S3...")

    # Initialize Spark Session
    # Notice we completely removed the spark.hadoop.fs.s3a.endpoint line!
    spark = SparkSession.builder \
        .appName("StockMarketAnalytics_Production") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    # Reduce console spam
    spark.sparkContext.setLogLevel("WARN")

    # Define the dynamic paths using s3a:// protocol
    silver_path = f"s3a://{bucket_name}/silver/*.parquet"
    gold_path = f"s3a://{bucket_name}/gold/daily_moving_averages"

    try:
        # 1. Read from the Silver Zone (AWS)
        logging.info(f"Reading Silver data from {silver_path}")
        df = spark.read.parquet(silver_path)

        # 2. Transform: Calculate Moving Averages
        logging.info("Calculating Average Close Price and Total Volume...")
        analytics_df = df.groupBy("ticker") \
            .agg(
                round(avg("close"), 2).alias("avg_close"),
                round(avg("volume"), 0).alias("avg_volume")
            )

        analytics_df.show()

        # 3. Write to the Gold Zone (AWS)
        logging.info(f"Writing Gold data to {gold_path}")
        analytics_df.write \
            .mode("overwrite") \
            .parquet(gold_path)

        logging.info("Spark Analytics Pipeline Completed Successfully!")

    except Exception as e:
        logging.error(f"Spark Job Failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    process_stock_data()