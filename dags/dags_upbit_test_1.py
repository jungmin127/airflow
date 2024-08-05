from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import pyupbit
import pandas as pd
import logging

# 기본 인자 설정
default_args = {
    'retries': 3,  # 전체 DAG에 대해 재시도 횟수 설정
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
}

# 함수 정의
def define_crypto(crypto, to_date, count):
    try:
        df = pyupbit.get_ohlcv(crypto, interval="minute3", to=to_date, count=count, period=0.1)
        if df is not None:
            df = df.reset_index()
            df = df.rename(columns={'index': 'date'})
            df['date'] = df['date'].dt.to_period(freq='D')
            df = df.set_index('date')
            return df
        else:
            raise ValueError("No data returned from pyupbit.get_ohlcv.")
    except Exception as e:
        logging.error(f"Error in define_crypto: {e}")
        raise

def get_solar_prices():
    try:
        crypto = 'KRW-SOL'
        to_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        count = 5
        df = define_crypto(crypto, to_date, count)
        if df is not None:
            logging.info(df.head())
        else:
            logging.warning("No data fetched.")
    except Exception as e:
        logging.error(f"Error in get_solar_prices: {e}")
        raise

# DAG 설정
with DAG(
    dag_id='dags_upbit_test_1',
    default_args=default_args,  # 전체 DAG에 대한 기본 인자 설정
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    start_date=pendulum.datetime(2024, 8, 5, tz='Asia/Seoul'),  # 시작일
    catchup=False,  # 과거 날짜의 실행을 건너뛰도록 설정
) as dag:

    # PythonOperator 설정
    task = PythonOperator(
        task_id='get_solar_prices',
        python_callable=get_solar_prices,
        retries=3,  # 태스크 단위로 재시도 횟수 설정 (기본값이 default_args의 설정을 덮어씀)
        retry_delay=timedelta(minutes=5),  # 재시도 간격
    )

    task