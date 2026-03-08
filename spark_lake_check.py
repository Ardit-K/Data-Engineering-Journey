from pyspark.sql import SparkSession
import os

# --- SPARK CONFIGURATION ---
# Using 3.3.4 and 1.12.262 to fix the '60s' NumberFormatException
SUBMIT_ARGS = (
    "--conf spark.driver.extraJavaOptions='-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000' "
    "--conf spark.executor.extraJavaOptions='-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000' "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "pyspark-shell"
)
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

def create_spark_session():
    spark = (SparkSession.builder
        .appName("ArditStockAnalytics")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())
    return spark

def analyze_stock_data():
    spark = create_spark_session()
    
    # Path to your S3 bucket
    s3_path = "s3a://ardit-stock-data-lake/*.parquet"
    
    print("\n--- ⚡ Spark is Reading from S3 ⚡ ---")
    
    try:
        # READ
        df = spark.read.parquet(s3_path)
        
        # SHOW DATA
        df.show(5)
        print(f"Total rows processed: {df.count()}")
        
        # ANALYTICS
        print("\n--- 📈 Average Price per Ticker ---")
        df.groupBy("ticker").avg("close_price").show()
        
    except Exception as e:
        print(f"❌ Spark failed to read from S3: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_stock_data()