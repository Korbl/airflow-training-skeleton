from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
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

def _return_task(**context):
    return weekday_person_to_email[datetime.today().weekday()]


dag = DAG('PythonBranch-operation', default_args=default_args, schedule_interval=timedelta(days=1))


start = PythonOperator(
    task_id='print_the_exec_date',
    provide_context=True,
    python_callable=_print_exec_date,
    dag=dag,
)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=_return_task,
    provide_context=True, dag=dag)

finish = BashOperator(
    task_id='finish',
    bash_command='echo "finished!"',
    retries=3,
    dag=dag)


#days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

weekday_person_to_email = {
    0: "email_Bob",
    1: "email_Joe",
    2: "email_Alice",
    3: "email_Joe",
    4: "email_Alice",
    5: "email_Alice",
    6: "email_Alice",
}

for day, name in weekday_person_to_email.items():
    start >> branching >> DummyOperator(task_id=name, dag=dag) >> finish
