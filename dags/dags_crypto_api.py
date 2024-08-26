from airflow import DAG
import pendulum
from common.operators.crypto_api_to_csv import CryptoToCSVOperator

coins = [
    'KRW-BTC', 'KRW-ETH', 'KRW-SOL', 'KRW-XRP', 'KRW-TRX', 'KRW-LINK', 'KRW-HBAR',
    'KRW-ETC', 'KRW-DOGE', 'KRW-ALGO', 'KRW-BSV', 'KRW-EOS', 'KRW-DOT', 'KRW-FLOW',
    'KRW-ADA', 'KRW-SC', 'KRW-SAND', 'KRW-UPP', 'KRW-QTUM'
]

with DAG(
    dag_id='dags_crypto_api',
    schedule='1 * * * *',
    start_date=pendulum.datetime(2024, 8, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:

    tasks = []
    for coin in coins:
        market = coin
        path = f'/opt/airflow/files/{market[4:].lower()}/'
        file_name = f'{market[4:].lower()}_data.csv'
        
        task = CryptoToCSVOperator(
            task_id=f'{market.lower()}_value',
            path=path,
            file_name=file_name,
            market=market
        )
        tasks.append(task)

    if tasks:
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]