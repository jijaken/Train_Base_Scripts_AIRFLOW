'''
Делаем селект из PostgreSQL использая Connections и BaseHook
'''

from datetime import timedelta, datetime
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor

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
with DAG('hw_10_n-eremenko',
         default_args=default_args,
         description='hw_10_',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 9, 9),
         catchup=False,
         tags=['hw_10_n-eremenko']) as dag:

    def get_conn():
        '''
        Фунция подключения к PostgreSQL и получение данных
        '''
        #Берем значения Connections по id 'startml_feed'
        creds = BaseHook.get_connection('startml_feed')
        #подключаемся к БД с RealDictCursor, чтобы забрать значение в виде словаря
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}@{creds.host}:{creds.port}/{creds.schema}",
                cursor_factory=RealDictCursor
        ) as conn:

            with conn.cursor() as cursor:
                #Делаем запрос и находим самого лайкавшего пользователя
                cursor.execute('''select user_id,count(action) 
                from feed_action 
                where action='like' 
                group by user_id 
                order by count(action) desc 
                limit 1;''')
                #Выводим результат
                return cursor.fetchone()

    t1 = PythonOperator(task_id='dag_10_get_connection',
                        python_callable=get_conn)

    t1