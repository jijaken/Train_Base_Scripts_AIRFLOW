'''
Вытаскиваем общие переменные на сверере через Variable
'''

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#Аргументы, которые будут использоваться для тасков по дефолту
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  # timedelta из пакета datetime
}
#Задаем параметры DAG
with DAG('hw_12_n-eremenko',
         default_args=default_args,
         description='hw_12_',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 9, 9),
         catchup=False,
         tags=['hw_12_n-eremenko']) as dag:

    def get_variable():
        '''
        Фкнция вытаскивает переменную из Variable
        '''
        from airflow.models import Variable
        print(Variable.get('is_startml'))

    t1 = PythonOperator(task_id='dag_12_get_variable',
                        python_callable=get_variable)

    t1