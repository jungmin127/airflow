from common.operators.bok_api_to_csv_operator import BokKospiToDataFrameOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_bok_api_kospi',
    schedule='0 21 * * *',
    start_date=pendulum.datetime(2024,8,8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''일별 Kospi값'''
    kospi_value = BokKospiToDataFrameOperator(
        task_id='kospi_value',
        #path='/opt/airflow/files/kospi_value/{{ ds_nodash }}',
        path='/opt/airflow/files/kospi_value/',
        file_name='kospi.csv'
    )

    kospi_value