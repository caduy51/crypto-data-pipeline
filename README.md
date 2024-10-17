# Crypto Data Pipeline

## Project structure
1. All source code sits in: /src/dags

2. Main program: /src/dags/crypto_pipeline.py

3. Local data of airflow: /src/data

4. Module
   -  /src/dags/binance: contains scripts related to Binance
   -  /src/dags/gecko: contains scripts related to Gecko
   -  /src/dags/helpers: contains credentials and helper function
   -  /src/dags//spark_app: contains spark script to process data from Minio
   -  /src/dags/staging: contains loading scripts from Minio to Postgres

## Introduction

The Crypto Data Pipeline is designed to efficiently aggregate and process cryptocurrency data from two key sources: the Binance API and CoinGecko. This robust system facilitates real-time data collection and analysis, enabling users to gain valuable insights into the ever-evolving cryptocurrency market.

## Features

- **Data Sources**: Integrates with the Binance API for real-time trading data and CoinGecko for comprehensive market information.
- **Data Processing**: Utilizes Apache Spark for scalable data processing, ensuring high performance and efficiency.
- **Workflow Management**: Managed with Apache Airflow, allowing for easy orchestration and scheduling of data tasks.
- **Storage Solutions**: Employs MinIO for scalable object storage and PostgreSQL for structured data storage.
- **Data Visualization**: Leverages Apache Superset to create interactive dashboards and visualizations, making it easy to analyze trends and patterns.

## Tools Used

- **Docker**: Containerization for seamless deployment and management.
- **Apache Airflow**: Workflow orchestration for scheduling and monitoring data pipelines.
- **Apache Spark**: Distributed data processing for handling large volumes of cryptocurrency data.
- **MinIO**: High-performance object storage for managing unstructured data.
- **PostgreSQL**: Reliable relational database for structured data storage.
- **Apache Superset**: Modern data visualization platform for creating insightful dashboards.

This pipeline empowers users to harness the power of cryptocurrency data, enabling informed decision-making and deeper market understanding.


