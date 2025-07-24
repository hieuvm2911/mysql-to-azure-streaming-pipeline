# Real-Time MySQL to Azure Streaming Data Pipeline

## Overview

This repository implements a real-time Change Data Capture (CDC) pipeline designed to capture, process, and load data from a MySQL database into Azure cloud data services. The pipeline leverages modern data engineering technologies and best practices for scalable, reliable, and maintainable data streaming.

**Key Features:**
- Real-time CDC from MySQL using Debezium (Kafka Connect)
- Durable, decoupled event streaming with Apache Kafka
- Scalable transformation and enrichment via Apache Spark Structured Streaming
- Data landing in Azure Data Lake (ADLS Gen2) and incremental loading into Azure SQL Data Warehouse
- Fully orchestrated and containerized using Apache Airflow and Docker Compose

---

## Architecture

```
MySQL (source DB)
     |
     v
Debezium (Kafka Connect)
     |
     v
Apache Kafka (CDC topics)
     |
     v
Apache Spark Structured Streaming
     |                            
     v                             
Azure Data Lake (ADLS Gen2)   -->  Azure SQL DWH (Fact & Dim tables)
```

---

## Tech Stack

- **Source Database:** MySQL 8.x
- **CDC Tool:** Debezium (MySQL Connector via Kafka Connect)
- **Message Broker:** Apache Kafka
- **Processing:** Apache Spark (Structured Streaming)
- **Orchestration:** Apache Airflow
- **Cloud Storage & DWH:** Azure Blob Storage, Azure Data Lake Gen2, Azure SQL Database
- **Containerization:** Docker, Docker Compose

---

## Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- Azure credentials for access to ADLS Gen2 and Azure SQL Database (connection details must be set via environment variables, e.g., `AZURE_STORAGE_KEY`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`)

---

## Quick Start

To start the entire pipeline, simply run:

```bash
docker compose up --build
```

This single command will:

- Build and start all services: MySQL, Zookeeper, Kafka, Kafka Connect (Debezium), Spark, Airflow, etc.
- Stand up the CDC flow from MySQL → Kafka → Spark → Azure
- Orchestrate Spark jobs via Airflow for continuous and scheduled data processing

---

## Pipeline Details

### 1. Change Data Capture (CDC)
- Debezium tails the MySQL binlog and publishes change events to Kafka topics.

### 2. Streaming & Transformation
- Spark Structured Streaming jobs consume Kafka CDC topics, parse Debezium event formats, and perform data cleaning/enrichment.
- Cleaned data is written to Azure Data Lake (ADLS Gen2) in Parquet format.

### 3. Data Warehouse Loading
- A scheduled Spark job (orchestrated by Airflow) incrementally loads processed data from ADLS into Azure SQL Data Warehouse.
- Fact and dimension tables are maintained for analytics.

---

## Directory Structure

```
.
├── airflow/                 # Airflow DAGs for orchestration
│   └── dags/
├── spark/                   # Spark jobs and configurations
│   ├── jobs/
│   ├── submit_to_adls.sh
│   ├── submit_to_dwh.sh
│   └── spark-defaults.conf
├── debezium-connect.dockerfile  # Builds Debezium connector image
├── dockerfile                   # Airflow custom image
├── requirements.txt             # Python dependencies for Spark/Airflow
├── README.md                    # (You are here)
└── ...
```

---

## Environment Variables

Make sure to set the following environment variables in your `.env` file or Docker Compose environment:

- `AZURE_STORAGE_KEY`: Access key for ADLS Gen2
- `AZURE_SQL_USERNAME`: Username for Azure SQL
- `AZURE_SQL_PASSWORD`: Password for Azure SQL

---

## Operational Notes

- All service networking is handled within Docker Compose; refer to services using their container names (e.g., `kafka:9092`, `spark-master:7077`).
- Airflow manages Spark job submission via BashOperator and cron-like scheduling.
- Spark jobs use only the Structured Streaming API (no DStream).

---

## Extending & Customization

- **Add new source tables:** Update Debezium connector config and Spark schema as needed.
- **Add new sinks:** Extend Spark jobs to write to additional Azure services or downstream systems.
- **Business logic:** Implement additional transformation/validation in the Spark streaming job.

---

## Troubleshooting

- **Logs:** Use `docker compose logs <service>` to inspect logs for any service.
- **Airflow UI:** Access Airflow UI at `http://localhost:<airflow_port>` for DAG status and logs.
- **Spark UI:** Access Spark UI at `http://localhost:<spark_port>` for job monitoring.
- **Kafka Topics:** Use Kafka UI at `http://localhost:8082` to inspect topics and messages if needed.

---

## License

MIT License

---

## Author

Maintained by [hieuvm2911] 
