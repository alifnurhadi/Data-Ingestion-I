from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Import your callable functions
from cleaning_logic import task_clean_customers, task_clean_sales

default_args = {
    "owner": "cleaning old datas",
    "depends_on_past": False,
    "retries": 2,
}

with (
    DAG(
        dag_id="clean_raw_tables",
        default_args=default_args,
        description="Cleans and recasts raw tables independently and on purposes",
        schedule_interval=None,  # Set to None for manual triggering, or add your cron schedule
        start_date=datetime(2026, 3, 1),
        catchup=False,
    ) as dag
):
    start = EmptyOperator(task_id="start")

    clean_sales = PythonOperator(
        task_id="clean_sales_data",
        python_callable=task_clean_sales,
    )

    clean_customers = PythonOperator(
        task_id="clean_customers_data",
        python_callable=task_clean_customers,
    )

    # HOW TO RUN INDEPENDENTLY:
    # We branch out from the start task.
    # Because clean_sales and clean_customers are in a list, they run in parallel!
    start >> [clean_sales, clean_customers]
