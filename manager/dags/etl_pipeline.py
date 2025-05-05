from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline triggering existing Docker containers',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['etl'],
) as dag:

    # Check if containers exist before running commands
    check_containers = BashOperator(
        task_id='check_containers',
        bash_command='docker ps | grep -q etl-extractor && docker ps | grep -q etl-transformer || exit 1',
    )

    # Use BashOperator to execute commands in existing containers
    run_extractor = BashOperator(
        task_id='run_extractor',
        bash_command='docker exec etl-extractor-1 python /app/main.py',
    )

    check_extraction = BashOperator(
        task_id='check_extraction',
        bash_command='echo "Checking extraction..."',
    )

    run_transformer = BashOperator(
        task_id='run_transformer',
        bash_command='docker exec etl-transformer-1 python /app/main.py',
    )

    clear_intermediate_table = BashOperator(
        task_id='clear_intermediate_table',
        bash_command='echo "Clearing intermediate table..."',
    )

    # Define the workflow
    check_containers >> run_extractor >> check_extraction >> run_transformer >> clear_intermediate_table