from airflow import DAG
import pendulum
from common.operators.fetch_crypto_data import FetchLatestTradePriceOperator 

coins = [
    'KRW-BTC', 'KRW-ETH', 'KRW-SOL', 'KRW-XRP', 'KRW-TRX', 'KRW-LINK', 'KRW-HBAR',
    'KRW-ETC', 'KRW-DOGE', 'KRW-ALGO', 'KRW-BSV', 'KRW-EOS', 'KRW-DOT', 'KRW-FLOW',
    'KRW-ADA', 'KRW-SC', 'KRW-SAND', 'KRW-UPP', 'KRW-QTUM'
]

with DAG(
    dag_id='dags_output_dash',
    start_date=pendulum.datetime(2024, 8, 18, tz='Asia/Seoul'),
    schedule='15 * * * *',
    catchup=False
) as dag:

    tasks = []
    for coin in coins:
        task = FetchLatestTradePriceOperator(
            task_id=f'buy_sell_return_dash_{coin.lower()}',
            table_name=coin,
            postgres_conn_id='conn-db-postgres-custom',
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]