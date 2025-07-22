from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'load_to_synapse_dag',
    default_args=default_args,
    description='Submit Spark job to load data to Synapse every 1 hour',
    schedule_interval='@hourly',  # or "0 * * * *"
    catchup=False,
) as dag:

    submit_spark_job = BashOperator(
        task_id='spark_submit_to_synapse',
        bash_command="""
        {{ 'docker exec spark-master bash /opt/spark/submit_to_synapse.sh' }}
    """
    )

    submit_spark_job
