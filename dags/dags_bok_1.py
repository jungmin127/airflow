from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_bok_1',
    start_date=pendulum.datetime(2024,8,6, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    
    '''한국은행경제통계시스템'''
    bok_kospi_info = HttpOperator(
        task_id='bok_test_1',
        http_conn_id='bok_api',
        endpoint='{{var.value.apikey_openapi_bok}}/json/kr/1/2000/802Y001/D/20240101/20240731/0001000',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='bok_test_1')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    bok_kospi_info >> python_2()