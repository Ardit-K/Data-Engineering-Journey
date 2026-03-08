Multi-Phase Data Engineering Pipeline: Stock Market Analytics
This project demonstrates a professional-grade Data Engineering lifecycle. It evolves from a local Python script into a Modern Data Lakehouse architecture, showcasing skills in relational modeling, columnar storage, and cloud-native simulation.

🏗 Project Architecture
This system utilizes a Hybrid Storage Strategy, simultaneously feeding a traditional relational database for business intelligence and a cloud-native Data Lake for big data analytics.

Phase 1: Relational Foundation
Star Schema Design: Normalized PostgreSQL database utilizing dim_stocks and fact_prices to optimize analytical query performance.

Defensive ETL: Custom ingestion framework built in Python using OOP principles (Inheritance, Type Hinting) with robust error handling for market data volatility.

Containerization: Full orchestration of the database environment using Docker, ensuring consistency across development environments.

Phase 2: Cloud-Native Data Lake
Medallion Architecture: Implementation of a tiered storage strategy (Silver Zone) to maintain data lineage and high-quality historical records.

Columnar Storage: Conversion of raw data into Apache Parquet format, utilizing PyArrow for high-speed compression and schema enforcement.

AWS Cloud Simulation: Integration of LocalStack to mock AWS S3 services, enabling cloud-ready development and boto3 API testing without infrastructure costs.

🛠 Tech Stack
Languages: Python 3.x (Pandas, PyArrow, Boto3, yFinance)

Database: PostgreSQL

Infrastructure: Docker & Docker Compose

Cloud Simulation: LocalStack (AWS S3 Mock)

Automation: Linux/macOS Crontab

🚀 Getting Started
1. Infrastructure Setup
Ensure Docker Desktop is running, then spin up the hybrid storage environment:

Bash
docker-compose up -d
2. Environment Configuration
Install the necessary Python libraries and the AWS CLI local wrapper:

Bash
pip install pandas pyarrow boto3 awscli-local
3. Pipeline Execution
Run the orchestrator to extract, transform, and load data into both Postgres and S3:

Bash
python3 main.py
4. Data Verification
To verify the data has reached the "Cloud," use the following command:

Bash
awslocal s3 ls s3://ardit-stock-data-lake/
📈 Roadmap
Phase 3 (Active): Distributed Processing with Apache Spark.

Phase 4: Workflow Orchestration with Apache Airflow.

Phase 5: Migration to AWS Production (Real S3 & Redshift).