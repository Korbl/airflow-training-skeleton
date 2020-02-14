import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from operators.httptogcsoperator import HttpToGcsOperator
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

dag = DAG('Postgres-operation', default_args=default_args, schedule_interval=timedelta(days=1))

upload_data = PostgresToGoogleCloudStorageOperator(
    postgres_conn_id='postgres_gcs',
    task_id="get_data",
    sql=SQL_QUERY,
    bucket=GCS_BUCKET,
    filename=FILENAME,
    gzip=False,
    dag=dag
)

fetch_conversion = HttpToGcsOperator(
    task_id="fetch_converstion",
    gcs_bucket=GCS_BUCKET,
    gcs_path="converstion.json",
    endpoint="history?start_at=2019-01-01&end_at=2020-01-01&symbols=EUR&base=GBP",
    dag=dag
)

create_cluster = DataprocClusterCreateOperator(
    task_id='CreateCluster',
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="afspfeb3-0f44dce6cdbd5bd3e4c47",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

destroy_cluster = DataprocClusterDeleteOperator(
    task_id='CreateCluster',
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="afspfeb3-0f44dce6cdbd5bd3e4c47",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

[upload_data, fetch_conversion] >> create_cluster >> destroy_cluster
