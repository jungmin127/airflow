from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.hooks.custom_crypto_postgres_hook import CustomCryptoPostgresHook

coins = [
    'KRW-BTC', 'KRW-ETH', 'KRW-SOL', 'KRW-XRP', 'KRW-TRX', 'KRW-LINK', 'KRW-BAR',
    'KRW-ETC', 'KRW-DOGE', 'KRW-ALGO', 'KRW-BSV', 'KRW-EOS', 'KRW-DOT', 'KRW-FLOW',
    'KRW-ADA', 'KRW-SC', 'KRW-SAND', 'KRW-UPP', 'KRW-QTUM'
]

def insrt_postgres(postgres_conn_id, market, **kwargs):
    file_path = f'/opt/airflow/files/{market[4:].lower()}/{market[4:].lower()}_data.csv'
    table_name = market[4:].lower()
    custom_postgres_hook = CustomCryptoPostgresHook(postgres_conn_id=postgres_conn_id)
    custom_postgres_hook.bulk_load(
        table_name=table_name,
        file_name=file_path,
        delimiter=',',
        is_header=True,
        is_replace=False
    )

with DAG(
    dag_id='dags_crypto_custom_hook',
    start_date=pendulum.datetime(2024, 8, 18, tz='Asia/Seoul'),
    schedule='3 * * * *',
    catchup=False
) as dag:

    tasks = []
    for coin in coins:
        task = PythonOperator(
            task_id=f'insrt_postgres_task_{coin.lower()}',
            python_callable=insrt_postgres,
            op_kwargs={
                'postgres_conn_id': 'conn-db-postgres-custom',
                'market': coin
            }
        )
        tasks.append(task)

    if tasks:
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]