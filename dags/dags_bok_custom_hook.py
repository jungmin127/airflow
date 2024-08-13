from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.hooks.custom_bok_postgres_hook import CustomBokPostgresHook

with DAG(
        dag_id='dags_bok_custom_hook',
        start_date=pendulum.datetime(2024,8,10, tz='Asia/Seoul'),
        schedule='1 21 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_bok_postgres_hook = CustomBokPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_bok_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'bok_kospi',
                   'file_nm':'/opt/airflow/files/kospi_value/kospi.csv'})

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'bok_koribo12',
                   'file_nm':'/opt/airflow/files/koribo12_value/_koribo12/_koribo12_koribo12.csv'})