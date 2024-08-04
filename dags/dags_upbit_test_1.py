from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import pyupbit
import pandas as pd

# 함수 정의
def define_crypto(crypto, to_date, count):
    df = pyupbit.get_ohlcv(crypto, interval="minute1", to=to_date, count=count, period=0.1)
    df = df.reset_index()
    df = df.rename(columns={'index': 'date'})
    df['date'] = df['date'].dt.to_period(freq='D')
    df = df.set_index('date')
    return df

def get_solar_prices():
    crypto = 'KRW-SOL'
    to_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 5
    df = define_crypto(crypto, to_date, count)
    print(df)  # 혹은 파일에 저장하거나 데이터베이스에 저장하는 등의 작업

# DAG 설정
with DAG(
    dag_id='dags_upbit_test_1',
    schedule_interval='*/1 * * * *',  # 1분마다 실행
    start_date=pendulum.datetime(2024, 8, 4, tz='Asia/Seoul'),  # 시작일
    catchup=False,  # 과거 날짜의 실행을 건너뛰도록 설정

) as dag:

    # PythonOperator 설정
    task = PythonOperator(
        task_id='get_solar_prices',
        python_callable=get_solar_prices
    )

    task
