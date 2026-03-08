from pyspark.sql import SparkSession
import os

# 1. Simplify Submit Args - Let's stick to the most compatible versions
SUBMIT_ARGS = "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"
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

    # Reach into the Java Hadoop Configuration and scrub the "24h" and "60s" values
    hadoop_conf = spark._jsc.hadoopConfiguration()
    
    # Scrubbing the "60s" culprits
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    
    # Scrubbing the "24h" culprits (Multipart Upload settings)
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400") # 24 hours in seconds
    hadoop_conf.set("fs.s3a.listing.cache.expiration", "600") # 10 minutes in seconds
    
    return spark

def analyze_stock_data():
    spark = create_spark_session()
    
    # Use the full path without the wildcard first to test connectivity
    s3_path = "s3a://ardit-stock-data-lake/"
    
    print("\n--- ⚡ Spark is Reading from S3 ⚡ ---")
    
    try:
        df = spark.read.option("mergeSchema", "false").parquet(s3_path)
        df.show(5)
        print(f"Total rows: {df.count()}")
    except Exception as e:
        print(f"❌ Spark failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_stock_data()