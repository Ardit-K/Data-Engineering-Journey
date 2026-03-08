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

---

## 🛠 Tech Stack

* **Languages:** Python 3.x (`Pandas`, `PyArrow`, `Boto3`, `yFinance`)
* **Database:** PostgreSQL (via Docker)
* **Cloud Simulation:** LocalStack (AWS S3 Mock)
* **Infrastructure:** Docker & Docker Compose
* **Automation:** Linux/macOS Crontab

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
pip install pandas pyarrow boto3 awscli-local
```

### 3. Pipeline Execution
Run the orchestrator to extract, transform, and load data into both Postgres and S3:
``` bash
python3 main.py
```

### 4. Data Verification
To verify the data has reached the "Cloud," use the awslocal CLI:
```bash
awslocal s3 ls s3://ardit-stock-data-lake/
```

---

## 📈 Roadmap
* Phase 3 (Work in Progress): Distributed Processing with Apache Spark.

* Phase 4: Workflow Orchestration with Apache Airflow.

* Phase 5: Migration to AWS Production (Real S3 & Redshift).