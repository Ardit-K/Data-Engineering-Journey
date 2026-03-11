# 📈 End-to-End Cloud-Native Stock Market Data Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow)
![PySpark](https://img.shields.io/badge/PySpark-3.5.3-E25A1C?logo=apachespark)
![AWS S3](https://img.shields.io/badge/AWS%20S3-Data%20Lake-569A31?logo=amazons3)
![AWS Redshift](https://img.shields.io/badge/AWS%20Redshift-Serverless-8C4FFF?logo=amazonredshift)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)

## 📌 Project Overview
This project is an automated, cloud-native data pipeline designed to extract, process, and warehouse daily stock market data for quantitative analysis. Built utilizing a **Medallion Architecture**, the pipeline orchestrates the ingestion of raw financial data, distributed processing using Apache Spark, and high-performance loading into an Amazon Redshift Serverless data warehouse.

The entire workflow is fully containerized and orchestrated via **Apache Airflow**, ensuring reliable daily execution and fault tolerance.

## 🏗️ Architecture & Data Flow

*(Note: Insert an image of your architecture diagram here. You can build a great one at draw.io or excalidraw.com!)*

The pipeline follows a strict extraction, transformation, and loading (ETL) sequence:

1. **Extraction:** Python scripts fetch live market data (Tickers: AAPL, MSFT, GOOGL, TSLA, NVDA) via the `yfinance` API.
2. **Local BI Database (Bronze):** Raw data is initially persisted in a local PostgreSQL database for immediate operational querying.
3. **Data Lake Ingestion (Silver):** Data is cleaned, partitioned, and written as Parquet files to an **AWS S3 Silver Zone**.
4. **Distributed Processing (Spark):** A PySpark engine reads the Silver Zone data in-memory, calculates daily moving averages and aggregate volumes, and writes the transformed analytics to an **AWS S3 Gold Zone**.
5. **Data Warehousing (Redshift):** A Python script executes an optimized Redshift `COPY` command, automatically ingesting the Gold Zone Parquet files into an **Amazon Redshift Serverless** data warehouse for downstream Business Intelligence and quantitative modeling.

## 🛠️ Technology Stack
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Processing Engine:** Apache Spark (PySpark 3.5.3)
* **Data Lake:** Amazon Simple Storage Service (S3)
* **Data Warehouse:** Amazon Redshift (Serverless)
* **Databases:** PostgreSQL (Local BI)
* **Languages & Libraries:** Python, Pandas, PyArrow, Boto3, psycopg2

## 🚀 Prerequisites & Local Setup

### 1. Environment Variables
Create a `.env` file in the root directory with your local and AWS credentials:
```text
# Local Database
DB_PASSWORD=your_local_pg_password

# AWS S3 Data Lake
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# AWS Redshift Serverless
REDSHIFT_HOST=your_namespace.redshift-serverless.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=your_redshift_password
REDSHIFT_ROLE_ARN=arn:aws:iam::123456789:role/your_role
```

### 2. Start the Infrastructure
The entire environment, including the Airflow webserver, scheduler, and local Postgres database, is containerized.
```bash
docker-compose up -d --build
```

### 3. Execute the Pipeline
1. Navigate to the Airflow UI at http://localhost:8080
2. Enable the `stock_market_pipeline` DAG.
3. Trigger the DAG manually to execute the end-to-end ingestion, Spark analytics, and Redshift warehousing tasks.

## 📂 Project Structure
├── dags/
│   └── stock_pipeline_dag.py     # Airflow DAG definition & task dependencies
├── data/                         # Local storage volumes for Postgres/Silver zones
├── main.py                       # Phase 1 & 2: API Extraction & S3 Silver Ingestion
├── spark_lake_check.py           # Phase 3 & 4: PySpark Processing & S3 Gold Output
├── redshift_setup.py             # Phase 5: Redshift Serverless COPY command
├── pipelines.py                  # Core OOP data extraction and loading classes
├── docker-compose.yml            # Container definitions for Airflow & Postgres
├── Dockerfile                    # Custom Airflow image with PySpark 3.5.3
├── requirements.txt              # Python dependencies
└── .env                          # Secret credentials (Not tracked in Git)