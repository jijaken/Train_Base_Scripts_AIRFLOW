'''
Скрипт создает 20 тасков через BashOperator и 10 тасков через PythonOperator
Добавлена документация к таскам
'''

from datetime import timedelta, datetime
from textwrap import dedent

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
with DAG('hw_3_n-eremenko',
         default_args = default_args,
         description ='hw_3_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_3_n-eremenko']) as dag:

    def print_for_20(task_number):
        print(f'task number is: {task_number}')

    #Создаем 10 таксков BashOperator
    # Значение порядкого номера цикла печаетается
    for i in range(10):
        t1 = BashOperator(task_id='bash_for_echo_'+str(i),
                          bash_command= f'echo {i}')
    # Создаем 20 таксков PythonOperator
    #Значение порядкого номера цикла передается переменной task_number и печаетется строка
    for i in range(20):
        t2 = PythonOperator(task_id='print_for_20_python_'+str(i),
                            python_callable=print_for_20,
                            op_kwargs= {'task_number':int(i)}) #Передаем переменные через op_kwargs
    #Документация к таску 1
    t1.doc_md = dedent('''
    ##Task docunebtation
    `echo {i}`
    _cursive_
    **jir**
    ''')
    # Документация к таску 2
    t2.doc_md = dedent('''
        ##Task docunebtation
        `print(f\'task number is: {task_number}\')`
        _cursive_
        **jir**
        ''')
    t1 >> t2