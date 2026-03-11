import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

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
        
        # Override the timeout strings to integers for hadoop-aws:3.3.6 compatibility
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") 
        
        # The specific fix for the initThreadPools crash:
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") 

        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate())

    return spark

def run_analytics_pipeline():
    # 1. Initialize the Spark Session
    spark = create_spark_session()

    # 2. Define your S3 LocalStack paths
    silver_path = "s3a://ardit-stock-data-lake/silver/"
    gold_path = "s3a://ardit-stock-data-lake/gold/"

    print("--- 📥 Reading from S3 Silver Zone ---")
    # 3. Load the data ingested during Phase 1/2
    try:
        df = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"Error reading from Silver Zone. Make sure Phase 2 has populated {silver_path}")
        print(f"Exception: {e}")
        return

    # Transformation Logic
    window_spec = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
    df_gold = df.withColumn("7_day_moving_avg", F.round(F.avg("close").over(window_spec), 2))
    df_gold = df_gold.withColumn("signal", F.when(F.col("close") > F.col("7_day_moving_avg"), "BULLISH").otherwise("BEARISH"))

    print("--- 💾 Saving to S3 Gold Zone ---")
    # Save the aggregated data back to the simulated cloud
    df_gold.write.mode("overwrite").parquet(gold_path)

    print("--- ✅ SUCCESS ---")
    df_gold.select("ticker", "date", "close", "7_day_moving_avg", "signal").orderBy(F.desc("date")).show(10)

if __name__ == "__main__":
    run_analytics_pipeline()