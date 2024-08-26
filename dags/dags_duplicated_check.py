from airflow import DAG
import pendulum
from common.operators.duplicated_check import CompareDataOperator

coins = [
    'KRW-BTC', 'KRW-ETH', 'KRW-SOL', 'KRW-XRP', 'KRW-TRX', 'KRW-LINK', 'KRW-HBAR',
    'KRW-ETC', 'KRW-DOGE', 'KRW-ALGO', 'KRW-BSV', 'KRW-EOS', 'KRW-DOT', 'KRW-FLOW',
    'KRW-ADA', 'KRW-SC', 'KRW-SAND', 'KRW-UPP', 'KRW-QTUM'
]

with DAG(
    dag_id='dags_duplicated_check',
    start_date=pendulum.datetime(2024, 8, 18, tz='Asia/Seoul'),
    schedule='3 * * * *',
    catchup=False
) as dag:

    tasks = []
    for coin in coins:
        file_path = f'/opt/airflow/files/{coin[4:].lower()}/'
        file_name = f'{coin[4:].lower()}_data.csv'
        table_name = coin[4:].lower()
        
        task = CompareDataOperator(
            task_id=f'compare_data_{coin.lower()}',
            path=file_path,
            file_name=file_name,
            table_name=table_name,
            postgres_conn_id='conn-db-postgres-custom' 
        )
        tasks.append(task)

    if tasks:
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]