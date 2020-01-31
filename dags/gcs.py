import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 25),
    'email': ['seth@ragnarok.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'postgres_conn_id': 'postgres_gcs'
    # 'schedule_interval': "15 08 * * *",
    # 'schedule_interval': timedelta(minutes=150)
    # 'schedule_interval': "@daily"
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


GCS_BUCKET = "seth-gdd-training"
FILENAME = "land_prices"
SQL_QUERY = "select * from land_registry_price_paid_uk LIMIT 100;"

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

dag = DAG('Python-operation', default_args=default_args, schedule_interval=timedelta(days=1))

upload_data = PostgresToGoogleCloudStorageOperator(
    task_id="get_data",
    sql=SQL_QUERY,
    bucket=GCS_BUCKET,
    filename=FILENAME,
    gzip=False,
    dag=dag
)
