import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'nannaphasb',
}


@task()
def print_hello():
    print("Hello World!")
    

@task()
def print_date():
    print(datetime.datetime.now())


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1))
def taskflow_dag():

    t1 = print_hello()
    t2 = print_date()

    # Fan-out Pipeline
    t3 = BashOperator(
        task_id="list_file_gcs",
        bash_command="gsutil ls"
    )
    
    t1 >> [t2, t3]

example_dag = taskflow_dag()