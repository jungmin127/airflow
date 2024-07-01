from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_show_templates',
    schedule='30 9 * * *',
    start_date=pendulum.datetime(2024, 6, 15, tz='Asia/Seoul'),
    catchup=True  
    #이 코드 작성일: 2024/7/1, catchup True 로 주고 15개 누적출력
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()