'''
Используем BranchPythonOperator для развилки пути тасков исходя из переменной Variable
'''

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator

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
with DAG('hw_13_n-eremenko',
         default_args=default_args,
         description='hw_13_',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 9, 9),
         catchup=False,
         tags=['hw_13_n-eremenko']) as dag:

    def get_variable():
        '''
        Функция развилки тасков
        '''
        from airflow.models import Variable
        var = Variable.get('is_startml')
        if var == 'True':
            return "startml_desc"
        else:
            return "not_startml_desc"

    def startml_desc_func():
        '''
        Развилка в случае если Variable.get('is_startml')=='True'
        '''
        print("StartML is a starter course for ambitious people")

    def not_startml_desc_func():
        '''
        Развилка в случае если Variable.get('is_startml')!='True'
        '''
        print("Not a startML course, sorry")

    #Вызываем DummyOperator для красивого построения схемы в AIRFLOW
    dummy_oper = DummyOperator(
        task_id="zero_step",
        trigger_rule="all_success",
    )
    #Вызываем BranchPythonOperator
    t1 = BranchPythonOperator(task_id='determine_course',
                              python_callable=get_variable)

    t2 = PythonOperator(task_id="startml_desc",
                              python_callable=startml_desc_func)

    t3 = PythonOperator(task_id="not_startml_desc",
                              python_callable=not_startml_desc_func)
    #Порядок тасков
    dummy_oper >> t1 >> [t2,t3]