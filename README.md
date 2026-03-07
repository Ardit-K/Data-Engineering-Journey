# Multi-Phase Data Engineering Pipeline: Stock Market Analytics

This project demonstrates an end-to-end Data Engineering lifecycle, evolving from a local ETL process to a modern Cloud Data Lakehouse architecture.

## Phase 1: Foundational ETL & Relational Modeling
**Goal:** Build a robust, automated pipeline to extract financial data and store it in a containerized Star Schema.

### Key Features
* **Custom Python Framework:** Built using OOP principles (Inheritance and Type Hinting) for modularity.
* **Defensive Engineering:** Implemented robust error handling for "NaN" values and price rounding to ensure 'Quant-level' data precision.
* **Star Schema Architecture:** Designed a normalized PostgreSQL database with `dim_stocks` and `fact_prices` to optimize analytical queries.
* **Containerization:** Orchestrated a PostgreSQL instance using **Docker Desktop**.
* **Automation:** Deployed a Linux **Cron Job** to handle daily scheduled ingestion at market close.

### Tech Stack
* **Language:** Python 3.x (yfinance, psycopg2)
* **Database:** PostgreSQL
* **Infrastructure:** Docker, Linux/WSL
* **Tooling:** Git, Crontab
