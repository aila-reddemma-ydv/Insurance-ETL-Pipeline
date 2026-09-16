# Insurance ETL Pipeline

## Project Overview

The Insurance ETL Pipeline is a data engineering project that processes insurance policy, premium, claims, and customer risk data. The project uses Kafka, PySpark, Hadoop, AWS S3, AWS Lambda, AWS Step Functions, and MySQL/RDS to build an end-to-end data processing workflow.

## Objectives

- Ingest insurance data using Kafka.
- Process and transform data using PySpark.
- Calculate final insurance premiums.
- Merge and validate insurance claims.
- Calculate customer risk scores.
- Store processed data in Amazon S3.
- Load processed data into MySQL/Amazon RDS.
- Automate processing using AWS Lambda and Step Functions.

## Technologies Used

- Python
- PySpark
- Apache Kafka
- Hadoop HDFS
- AWS S3
- AWS Lambda
- AWS Step Functions
- Amazon RDS MySQL
- Pandas
- MySQL Connector
- python-dotenv
- Git and GitHub

## Project Structure

```text
Insurance ETL Pipeline/
│
├── data/
│   └── create_claims_data.py
│
├── kafka/
│   ├── producer.py
│   ├── consumer.py
│   ├── claims_producer.py
│   └── claims_consumer.py
│
├── databricks/
│   ├── insurance_etl.py
│   ├── claims_etl.py
│   └── risk_etl.py
│
├── database_loader.py
├── requirements.txt
├── doc.txt
├── README.md
├── .env
├── insurance-data/
└── output/
