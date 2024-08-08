from common.operators.bok_api_to_csv_operator import BokKospiToDataFrameOperator
from airflow import DAG
import pendulum

import os

# 디렉토리 경로 설정
directory_path = '/opt/airflow/files/kospi_value/'

# 디렉토리가 없는 경우 생성
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

with DAG(
    dag_id='dags_bok_api_kospi',
    schedule='0 20 * * *',
    start_date=pendulum.datetime(2024,8,7, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''일별 Kospi값'''
    kospi_value = BokKospiToDataFrameOperator(
        task_id='kospi_value',
        path='/opt/airflow/files/kospi_value/{{ ds_nodash }}',
        file_name='kospi.csv'
    )

    kospi_value