from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 29),
    'email': ['seth@ragnarok.net'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('rocket_launch', default_args=default_args, schedule_interval=timedelta(days=1))

download_rocket_launches = LaunchLibraryOperator(
    task_id="download_rocket_launches",
    conn_id="launchlibrary",
    endpoints="launch",
    params={"startdate": "{{ds}}", "enddate": "{{ tomorrow_ds}}"},
    result_path='/data/rocket_launches/ds={{ ds }}',
    result_filename="launches_json",
    dag=dag
)
