from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pyupbit

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_bitcoin_candles(**kwargs):
    # Upbit의 API를 이용해 비트코인의 최근 1분봉 5개를 가져옵니다.
    df = pyupbit.get_ohlcv("KRW-BTC", interval="minute1", count=5)
    print(df)

with DAG(
    dag_id='dags_upbit_test_2',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Every minute
    catchup=False,
) as dag:

    fetch_candles_task = PythonOperator(
        task_id='fetch_bitcoin_candles',
        python_callable=fetch_bitcoin_candles,
    )

    fetch_candles_task