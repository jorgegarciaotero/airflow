from builtins import range

from datetime import timedelta,datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'Airflow',
    'start_date': datetime(2022,11,18, 19,50,00)
}


def helloWorldLoop():
    for palabra in ['hello','world']:
        print(palabra)

with DAG(dag_id='basic_dag',
    default_args=default_args,
    schedule_interval='@once') as dag:
    
    start=DummyOperator(task_id='start')
    
    prueba_python = PythonOperator(
        task_id='prueba_python',
        python_callable=helloWorldLoop
    )
    
    prueba_bash=BashOperator(
        task_id='prueba_bash',
        bash_command='echo prueba_bash'
    )

start >> prueba_python >> prueba_bash