from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
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
    # 'schedule_interval': "15 08 * * *",
    # 'schedule_interval': timedelta(minutes=150)
    # 'schedule_interval': "@daily"
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def _print_exec_date(**context):
    print(context["execution_date"])


dag = DAG('Pyton-operation', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = PythonOperator(
    task_id='print_the_exec_date',
    provide_context=True,
    python_callable=_print_exec_date,
    dag=dag,


)

t2 = BashOperator(
    task_id='wait_1',
    bash_command='sleep 1',
    retries=3,
    dag=dag)

t3 = BashOperator(
    task_id='wait_5',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

t4 = BashOperator(
    task_id='wait_10',
    bash_command='sleep 10',
    retries=3,
    dag=dag)

t5 = DummyOperator(
    task_id='Done',
    dag=dag)


t1 >> t2 >> [t3, t4] >> t5
