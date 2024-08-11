from airflow import DAG
import pendulum
from common.operators.bok_api_to_csv_features import BokApiToDataFrameOperator

with DAG(
    dag_id='dags_bok_api_all',
    schedule='0 21 * * *',
    start_date=pendulum.datetime(2024,8,8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''일별 Kospi값'''
    kospi_value = BokApiToDataFrameOperator(
        task_id='kospi_value',
        path='/opt/airflow/files/kospi_value/',
        file_name_template='{name}_kospi.csv',
        indicators=[{'code': '802Y001', 'name': 'kospi'}],
        data_type_code='0001000' 
    )
    '''일별 koribo12값'''
    koribo12_value = BokApiToDataFrameOperator(
        task_id='koribo12_value',
        path='/opt/airflow/files/koribo12_value/',
        file_name_template='{name}_koribo12.csv',
        indicators=[{'code': '817Y002', 'name': '_koribo12'}],
        data_type_code='010152000' 
    )

    kospi_value >> koribo12_value