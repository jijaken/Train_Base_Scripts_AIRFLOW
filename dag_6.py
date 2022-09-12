'''
Задаем переменную окружения NUMBER в BashOperator и выводим её
'''

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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
with DAG('hw_6_n-eremenko',
         default_args = default_args,
         description ='hw_6_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_6_n-eremenko']) as dag:



    #Перается пременная окржения и печатается
    for i in range(10):
        t1 = BashOperator(task_id='bash_for_echo_'+str(i),
                          bash_command= 'echo $NUMBER',
                          env={'NUMBER':i}) #переменная окружения


    t1