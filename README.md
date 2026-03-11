# Multi-Phase Data Engineering Pipeline: Stock Market Analytics

This project demonstrates a professional-grade Data Engineering lifecycle, evolving from a local Python script into a **Modern Data Lakehouse** architecture. It showcases a hybrid approach to data storage, balancing relational SQL for BI and columnar object storage for Big Data analytics.

---

## 🏗 Project Architecture

The system implements a **Dual-Ingestion Pattern**, ensuring data is stored in both a structured database and a scalable Data Lake simultaneously.

### Phase 1: Relational Foundation
* **Star Schema Design:** Implemented a normalized PostgreSQL database using `dim_stocks` and `fact_prices` to optimize analytical query performance.
* **Defensive ETL:** Built a robust ingestion framework using Python **OOP principles**, featuring automated error handling for market data volatility and data type validation.
* **Containerization:** Orchestrated the database environment using **Docker**, ensuring high availability and local environment consistency.

### Phase 2: Cloud-Native Data Lake
* **Medallion Architecture:** Applied a tiered storage strategy (**Silver Zone**) to maintain high-quality, historical data lineage.
* **Columnar Storage:** Optimized storage efficiency by converting raw data into **Apache Parquet** format using **PyArrow** for high-speed compression.
* **AWS Cloud Simulation:** Integrated **LocalStack** to mock **AWS S3** services, enabling `boto3` API integration and cloud-ready testing without infrastructure overhead.

### Phase 3: Distributed Analytics Engine
* **Big Data Processing:** Implemented Apache Spark (via PySpark) to read and process historical market data directly from the S3 data lake.

* **Quantitative Signal Generation:** Engineered window functions to calculate rolling 7-day moving averages and generate automated Bullish/Bearish market indicators.

* **Medallion Completion:** Routed the highly refined, aggregated trading signals back into the Gold Zone for downstream consumption and backtesting.

### Phase 4: Workflow Orchestration
* **Automated DAGs:** Implemented **Apache Airflow** to orchestrate the entire ETL and analytical pipeline, ensuring strict task dependencies and retry logic.
* **Isolated Execution:** Built custom Docker images containing all Python dependencies and Java runtimes, isolating the execution environment from the host machine.
* **Idempotent Design:** Engineered the pipeline to be fully idempotent, preventing data duplication in both PostgreSQL and S3 during manual re-runs or backfilling.

---

## 🛠 Tech Stack

* **Languages:** Python 3.11
* **Core Libraries:** `PySpark`, `Pandas`, `PyArrow`, `Boto3`, `yFinance`
* **Databases:** PostgreSQL (BI & Airflow Metadata)
* **Cloud Simulation:** LocalStack (AWS S3 Mock)
* **Orchestration:** Apache Airflow
* **Infrastructure:** Docker & Docker Compose

---

## 🚀 Getting Started

### 1. Infrastructure Setup
Ensure Docker Desktop is running, then spin up the hybrid storage environment:
```bash
docker-compose up -d
```

### 2. Environment Configuration
Install the necessary Python libraries and the AWS CLI local wrapper:
```bash
pip install pandas pyarrow boto3 awscli-local pyspark
```

### 3. Pipeline Execution
Run the orchestrator to extract, transform, and load data into both Postgres and S3:
``` bash
python3 main.py
```

### 4. Distributed Processing (Phase 3)
Execute the spark job to generate quantitative signals and populate the Gold Zone:
```bash
python3 spark_lake_check.py
```

### 5. Data Verification
To verify the data has reached the "Cloud," use the awslocal CLI:
```bash
# Check raw ingestion (Silver)
awslocal s3 ls s3://ardit-stock-data-lake/silver/

# Check analytical output (Gold)
awslocal s3 ls s3://ardit-stock-data-lake/gold/
```

---

## 📈 Roadmap
* Phase 5 (next): Migration to AWS Production (Real S3 & Redshift).