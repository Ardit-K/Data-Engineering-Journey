from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default behavior for your tasks
default_args = {
    'owner': 'ardit',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='Daily stock market ingestion and Spark processing',
    schedule_interval='30 16 * * 1-5', # Runs at 4:30 PM, Monday through Friday
    catchup=False,
    tags=['stocks', 'medallion_architecture']
) as dag:

    # Task 1: Trigger your Phase 1/2 Ingestion
    ingest_to_silver = BashOperator(
        task_id='run_python_ingestion',
        # We use the mounted /opt/airflow/project/ path inside the container
        bash_command='python3 /opt/airflow/project/main.py' 
    )

    # Task 2: Trigger your Phase 3 Spark Analytics
    process_to_gold = BashOperator(
        task_id='run_spark_analytics',
        bash_command='python3 /opt/airflow/project/spark_lake_check.py'
    )

    # Set the strict execution dependency
    ingest_to_silver >> process_to_gold

    # Create the new Redshift task
    load_to_redshift = BashOperator(
        task_id='load_to_redshift',
        bash_command='python3 /opt/airflow/project/redshift_setup.py',
    )

    # Update the Dependency Chain
    # This tells Airflow: Ingestion -> Spark -> Redshift
    ingest_to_silver >> process_to_gold >> load_to_redshift