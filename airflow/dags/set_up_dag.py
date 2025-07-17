from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

import requests
import json

KAFKA_CONNECT_URL = "http://debezium:8083/connectors"
CONNECTOR_NAME = "mysql-cdc-connector"
CONFIG_PATH = "/opt/airflow/kafka/debezium_mysql_connector.json"

def register_connector():
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        print(f"Connector `{CONNECTOR_NAME}` is already registered")
        return

    headers = {"Content-Type": "application/json"}
    response = requests.post(KAFKA_CONNECT_URL, headers=headers, json=config)

    if response.status_code in [200, 201]:
        print(" Connector registered successfully")
    else:
        raise Exception(f" Error: {response.status_code} - {response.text}")
    

with DAG(
    dag_id="start_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    description="Register Debezium connector into Kafka Connect and start Spark job to process Kafka messages",
    tags=["pipeline", "kafka", "spark", "azure"],
) as dag:
    register_connect = PythonOperator(
        task_id="register_connector",
        python_callable=register_connector
    )

    spark_submit_command = """
    docker exec spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-azure:3.3.1 \
    --conf spark.hadoop.fs.azure.account.auth.type.sqlstream.dfs.core.windows.net=SharedKey \
    --conf spark.hadoop.fs.azure.account.key.sqlstream.dfs.core.windows.net=${AZURE_STORAGE_KEY} \
    /opt/spark/jobs/kafka_to_azure.py
    """

    start_spark_job = BashOperator(
        task_id="start_spark_job",
        bash_command= spark_submit_command
    )

    register_connect >> start_spark_job