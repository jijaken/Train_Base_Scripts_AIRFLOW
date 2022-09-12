'''
Передаем переменные между тасками через XCom
В качестве пуша в ti будем делать return
'''

from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

#Аргументы, которые будут использоваться для тасков по дефолту
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  # timedelta из пакета datetime
}
#Задаем параметры DAG
with DAG('hw_10_n-eremenko',
         default_args = default_args,
         description ='hw_10_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_10_n-eremenko']) as dag:

    def push_xcom():
        '''
        Делаем пуш в ti в виде return
        '''
        return "Airflow tracks everything"


    def pull_xcom(ti):
        '''
        Делаем пулл переменной по ключу 'return value' и task_id
        '''
        xcom_value = ti.xcom_pull(
            key="return_value",
            task_ids="dag_10_push")
        print(xcom_value)



    t1 = PythonOperator(task_id='dag_10_push',
                        python_callable=push_xcom)

    t2 = PythonOperator(task_id='dag_10_pull',
                        python_callable=pull_xcom)

    t1 >> t2