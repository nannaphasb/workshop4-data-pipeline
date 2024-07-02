import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'nannaphasb',
}

# The Task Flow API is a part of Apache Airflow that simplifies writing DAGs (Directed Acyclic Graphs) and is well-suited for PythonOperator.
@task()
def print_hello():
    """
    Print Hello World!
    """
    print("Hello World!")
    

@task()
def print_date():
    """
    Print current date
    ref: https://www.w3schools.com/python/python_datetime.asp
    """
    print(datetime.datetime.now())


@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1))
def taskflow_dag():

    t1 = print_hello()
    t2 = print_date()

    t1 >> t2


example_dag = taskflow_dag()