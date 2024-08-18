from airflow import DAG
import pendulum
from common.operators.btc_api_to_csv import BTCtoCSVOperator

with DAG(
    dag_id='dags_btc_api',
    schedule='1 * * * *',
    start_date=pendulum.datetime(2024,8,18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    '''btc 1시간봉'''
    btc_value = BTCtoCSVOperator(
        task_id='btc_value',
        path='/opt/airflow/files/btc/',
        file_name='btc_data.csv'
    )

    btc_value