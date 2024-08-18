from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.hooks.custom_btc_postgres_hook import CustomBtcPostgresHook

with DAG(
        dag_id='dags_btc_custom_hook',
        start_date=pendulum.datetime(2024,8,18, tz='Asia/Seoul'),
        schedule='2 * * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_bok_postgres_hook = CustomBtcPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_bok_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=False)

    insrt_postgres_task1 = PythonOperator(
        task_id='insrt_postgres_task1',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'btc',
                   'file_nm':'/opt/airflow/files/btc/btc_data.csv'})