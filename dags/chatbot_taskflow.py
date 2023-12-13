from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'tharhtet',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    dag_id='check_scheduler_interval_1',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=days_ago(2, minute=15),
    schedule_interval=timedelta(minutes=1)
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2 and will be running after task1!"
    )

    task3 = BashOperator(
        task_id='thrid_task',
        bash_command="echo hey ok, I am task3 and will be running after task1 at the same time as task2!"
    )
    task1 >> [task2,task3]
    task3 >> task2
