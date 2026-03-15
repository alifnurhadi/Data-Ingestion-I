import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from ingest_logic import task_ingest

logger = logging.getLogger(__name__)

BASE_FOLDER = Variable.get("custaddr_folder")
MAPPING_PATH = Variable.get("custaddr_mapping")
MYSQL_CONN_STR = Variable.get("custaddr_mysql_conn")
TARGET_TABLE = Variable.get("custaddr_target_table")
ARCHIVE_FOLDER = Variable.get("custaddr_archive_folder")
MYSQL_AIRFLOW_CONN_ID = "Astroworld"

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="auto_ingest_customer_address",
    default_args=default_args,
    description="Auto ingest — triggered when daily CSV lands in folder",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["ingest", "customer", "auto"],
    template_searchpath=["/opt/airflow/dags/datamart/"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_daily_csv",
        fs_conn_id="custaddr_fs",
        filepath=f"{BASE_FOLDER}/customer_address_{{{{ ds_nodash }}}}.csv",
        poke_interval=60,
        timeout=60 * 60 * 6,
        mode="reschedule",
        soft_fail=False,
    )

    # Task 2 — run ingest pipeline
    ingest = PythonOperator(
        task_id="ingest_customer_address",
        python_callable=task_ingest,
        op_kwargs={
            "base_folder": BASE_FOLDER,
            "mapping_path": MAPPING_PATH,
            "conn_str": MYSQL_CONN_STR,
            "target_table": TARGET_TABLE,
            "archive_folder": ARCHIVE_FOLDER,
            "execution_date": "{{ ds }}",
        },
    )
    updt_sales_mart = SQLExecuteQueryOperator(
        task_id="update_sales_datamart",
        conn_id=MYSQL_AIRFLOW_CONN_ID,
        sql="sales_monthly_report.sql",
    )

    updt_service_mart = SQLExecuteQueryOperator(
        task_id="update_service_datamart",
        conn_id=MYSQL_AIRFLOW_CONN_ID,
        sql="yearly_report.sql",
    )

    wait_for_file >> ingest

    ingest >> [updt_sales_mart, updt_service_mart]
