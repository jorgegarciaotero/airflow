from builtins import range

from datetime import timedelta,datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'Airflow',
    'start_date': datetime(2022,11,18, 19,50,00),
    'depends_on_past': False,
    'email': ['jorgegarciaotero@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


def helloWorldLoop():
    for palabra in ['hello','world']:
        print(palabra)

with DAG(dag_id='01_basic_dag',
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