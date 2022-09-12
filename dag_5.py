'''
В BashOperator создаем скрипт Jinja, который в цикле печатает ts и run_id
'''

from datetime import timedelta, datetime
from textwrap import dedent

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
with DAG('hw_5_n-eremenko',
         default_args = default_args,
         description ='hw_5_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_5_n-eremenko']) as dag:

    def print_for_20(task_number):
        print(f'task number is: {task_number}')

    #Команда цикла реализованного через Jinja
    bash_commands =dedent('''
    {% for i in range(5) %}
        echo {{ ts }}
        echo {{ run_id }}
    {% endfor %}''')

    #Выозов команды в виде цикла
    t1 = BashOperator(task_id='bash_for_echo_',
                      bash_command= bash_commands)

    #Документация таска
    t1.doc_md = dedent('''
    ##Task docunebtation
    `{% for i in range(10) %}
        echo{{ i }}
    {% endfor %}`
    _cursive_
    **jir**
    ''')

    t1