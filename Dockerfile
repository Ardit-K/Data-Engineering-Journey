# Start from the official Airflow image
FROM apache/airflow:2.8.2-python3.11

# Switch to root user to install system-level dependencies
USER root

# Install Java (Required for PySpark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the Java Home environment variable so Spark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Switch back to the safe airflow user
USER airflow

# Install all the Python libraries your specific pipeline needs
RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    pyarrow \
    boto3 \
    pyspark \
    psycopg2-binary