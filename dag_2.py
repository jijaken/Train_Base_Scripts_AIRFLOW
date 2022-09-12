'''
Скрипт печетает в темринале AIRFLOW путь до папки, в которой он находится и переменную ds
(через Bash и Python операторы соответсвенно)
'''

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#Аргументы, которые будут использоваться для тасков по дефолту
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
#Задаем параметры DAG
with DAG('hw_2_n-eremenko',
         default_args = default_args,
         description ='hw_2_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_2_n-eremenko']) as dag:


    def context_print(ds):
        '''
        Функция Python для PythonOperator
        '''
        print(ds)
        return ds

    #Оператор Bash
    t1 = BashOperator(task_id='pwd_bash',
                      bash_command= 'pwd') #Вызываем команду
    # Оператор Python
    t2 = PythonOperator(task_id='print_python',
                        python_callable=context_print) #Вызываем функцию