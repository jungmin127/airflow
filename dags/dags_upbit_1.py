from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pyupbit
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Define the function that will be used by the Airflow task
def define_crypto(crypto, to_date, count):
    df = pyupbit.get_ohlcv(crypto, interval="minute1", to=to_date, count=count, period=0.1)
    df = df.reset_index()
    df = df.rename(columns={'index': 'date'})
    df['date'] = df['date'].dt.to_period(freq='D')
    df = df.set_index('date')
    print(df)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 5),  # Adjust the start date as needed
}

# Define the DAG
dag = DAG(
    dag_id='dags_upbit_1',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Run every minute
    catchup=False,
)

# Define the task
task_1 = PythonOperator(
    task_id='fetch_crypto_data',
    python_callable=define_crypto,
    op_args=['KRW-SOL', datetime.now().strftime('%Y-%m-%d'), 5],
    dag=dag,
)

task_1