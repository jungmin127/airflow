from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

class CustomCryptoPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id
    
    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id) 
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn
    
    def create_table_if_not_exists(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            market VARCHAR(50),
            candle_date_time_utc TIMESTAMP,
            candle_date_time_kst TIMESTAMP UNIQUE,
            opening_price NUMERIC,
            high_price NUMERIC,
            low_price NUMERIC,
            trade_price NUMERIC,
            timestamp NUMERIC,
            candle_acc_trade_price NUMERIC,
            candle_acc_trade_volume NUMERIC,
            unit NUMERIC
        );
        """
        conn = self.get_conn()
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        self.create_table_if_not_exists(table_name)
        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)

        header = 0 if is_header else None
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)
        file_df = file_df.drop_duplicates()

        for col in file_df.columns:
            try:
                file_df[col] = file_df[col].astype(str).str.replace('\r\n', '', regex=False)
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue

        if not is_replace:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}')
            with engine.connect() as conn:
                existing_data_query = f"SELECT candle_date_time_kst FROM {table_name};"
                existing_data = pd.read_sql(existing_data_query, conn)
            
            file_df = file_df[~file_df['candle_date_time_kst'].isin(existing_data['candle_date_time_kst'])]

        self.log.info('중복 제거 후 적재 건수:' + str(len(file_df)))

        if not file_df.empty:
            uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
            engine = create_engine(uri)
            try:
                file_df.to_sql(name=table_name,
                               con=engine,
                               schema='public',
                               if_exists='append',
                               index=False,
                               method='multi')
                self.log.info(f"{table_name}에 데이터 적재 완료")
            except Exception as e:
                self.log.error(f"데이터 적재 중 오류 발생: {e}")
        else:
            self.log.info(f"{table_name}에 추가할 새 데이터가 없음")