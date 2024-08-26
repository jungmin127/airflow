from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine

class CompareDataOperator(BaseOperator):

    def __init__(self, path, file_name, table_name, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.file_name = file_name
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        return psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)

    def execute(self, context):
        self.log.info("Executing CompareDataOperator...")
        conn = self.get_conn()

        file_path = os.path.join(self.path, self.file_name)
        if not os.path.exists(file_path):
            self.log.error(f"파일이 존재하지 않음: {file_path}")
            return
        
        file_df = pd.read_csv(file_path, encoding='utf-8')
        file_df['candle_date_time_kst'] = pd.to_datetime(file_df['candle_date_time_kst'])

        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}')
        with engine.connect() as connection:
            existing_data_query = f"SELECT candle_date_time_kst FROM {self.table_name};"
            existing_data = pd.read_sql(existing_data_query, connection)
        
        existing_dates = set(existing_data['candle_date_time_kst'])
        file_dates = set(file_df['candle_date_time_kst'])

        unique_dates = file_dates - existing_dates
        unique_df = file_df[file_df['candle_date_time_kst'].isin(unique_dates)]

        self.log.info(f"중복되지 않는 데이터 건수: {len(unique_df)}") # 수동 trigger 시점에 체크
        self.log.info(f"중복되지 않는 데이터 Raw:\n{unique_df}")