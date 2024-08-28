from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
import pytz
from datetime import datetime, timedelta, timezone

class FetchLatestTradePriceOperator(BaseOperator):
    def __init__(self, table_name, postgres_conn_id, dash_api_url, **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.dash_api_url = dash_api_url

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port
        return psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)

    def format_table_name(self, table_name):
        return table_name.split('-')[1].lower()

    def execute(self, context):
        crypto_table_name = self.format_table_name(self.table_name)
        self.log.info(f"Fetching latest trade price from table {crypto_table_name}...")

        conn = self.get_conn()
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}')
        
        query = f"""
        SELECT market, candle_date_time_kst, trade_price
        FROM {crypto_table_name}
        ORDER BY candle_date_time_kst
        LIMIT 120;  
        """

        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        iloc_ranges = [24, 36, 48, 60, 120]
        rolling_windows = [5, 10, 15]
        filter_buy_df = pd.DataFrame(columns=['crypto', '(n)ma', 'datetime', 'close', 'ma'])

        cryptos = df['market'].unique()
        for crypto in cryptos:
            crypto_df = df[df['market'] == crypto].copy()
            for iloc_range in iloc_ranges:
                for rolling_window in rolling_windows:
                    temp_df = crypto_df[['trade_price']].iloc[iloc_range:]
                    temp_df['MA'] = temp_df['trade_price'].rolling(rolling_window).mean().shift(1)
                    temp_df['ACTION'] = np.where(temp_df['trade_price'] > temp_df['MA'], 'buy', 'sell')

                    cond_buy = (temp_df['ACTION'] == 'buy') & (temp_df['ACTION'].shift(1) == 'sell')
                    df_buy = temp_df[cond_buy].reset_index(drop=True)
                    df_buy.columns = ['datetime', 'close(buy)', 'MA', 'ACTION']
                    df_buy['datetime'] = crypto_df['candle_date_time_kst'].iloc[cond_buy].values

                    if not df_buy.empty:
                        last_date_buy = df_buy.tail(1)['datetime'].values[0]
                        last_day_buy = pd.to_datetime(last_day_buy)

                        kst = pytz.timezone('Asia/Seoul')
                        last_day_buy_kst = last_day_buy.tz_localize(kst, ambiguous='NaT')
                        #utc_time = datetime.utcnow()
                        utc_time = datetime.now(timezone.utc)
                        utc_time = pytz.utc.localize(utc_time)
                        kst_time = utc_time.astimezone(kst)

                        def get_previous_hour(dt):
                            return dt.replace(minute=0, second=0, microsecond=0)

                        previous_hour_time = get_previous_hour(kst_time)

                        if last_day_buy_kst == previous_hour_time:
                            buy_row = pd.DataFrame({
                                'crypto': [crypto],
                                '(n)ma': [rolling_window],
                                'datetime': [last_date_buy],
                                'close': [df_buy.tail(1)['close(buy)'].values[0]],
                                'ma': [temp_df['MA'].iloc[-1]]
                            })
                            filter_buy_df = pd.concat([filter_buy_df, buy_row], ignore_index=True)
                            filter_buy_df = filter_buy_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)