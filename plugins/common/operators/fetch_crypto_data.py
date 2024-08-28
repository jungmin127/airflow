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
    def __init__(self, table_name, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
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
        filter_sell_df = pd.DataFrame(columns=['crypto', '(n)ma', 'datetime', 'close', 'ma'])
        filter_return_df = pd.DataFrame(columns=['crypto', '(n)ma', 'day', 'date', 'return(%)'])

        cryptos = df['market'].unique()
        for crypto in cryptos:
            crypto_df = df[df['market'] == crypto].copy()
            for iloc_range in iloc_ranges:
                for rolling_window in rolling_windows:
                    temp_df = crypto_df[['candle_date_time_kst','trade_price']].iloc[iloc_range:].copy()
                    temp_df['MA'] = temp_df['trade_price'].rolling(rolling_window).mean().shift(1)
                    temp_df['ACTION'] = np.where(temp_df['trade_price'] > temp_df['MA'], 'buy', 'sell')

                    cond_buy = (temp_df['ACTION'] == 'buy') & (temp_df['ACTION'].shift(1) == 'sell')
                    df_buy = temp_df[cond_buy].reset_index(drop=True)
                    df_buy.columns = ['candle_date_time_kst','trade_price', 'MA', 'ACTION']
                    #df_buy['datetime'] = crypto_df['candle_date_time_kst'].iloc[cond_buy].values

                    if not df_buy.empty:
                        last_date_buy = df_buy.tail(1)['candle_date_time_kst'].values[0]
                        last_date_buy = pd.to_datetime(last_date_buy)

                        kst = pytz.timezone('Asia/Seoul')
                        last_date_buy_kst = last_date_buy.tz_localize(kst, ambiguous='NaT')
                        utc_time = datetime.now(timezone.utc)
                        kst_time = utc_time.astimezone(kst)

                        def get_previous_hour(dt):
                            return dt.replace(minute=0, second=0, microsecond=0)

                        previous_hour_time = get_previous_hour(kst_time)

                        if last_date_buy_kst == previous_hour_time:
                            buy_row = pd.DataFrame({
                                'crypto': [crypto],
                                '(n)ma': [rolling_window],
                                'datetime': [last_date_buy],
                                'close': [df_buy.tail(1)['trade_price'].values[0]],
                                'ma': [temp_df['MA'].iloc[-1]]
                            })
                            filter_buy_df = pd.concat([filter_buy_df, buy_row], ignore_index=True)
                            #filter_buy_df = filter_buy_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)

        for crypto in cryptos:
            crypto_df = df[df['market'] == crypto].copy()
            for iloc_range in iloc_ranges:
                for rolling_window in rolling_windows:
                    temp_df = crypto_df[['candle_date_time_kst','trade_price']].iloc[iloc_range:].copy()
                    temp_df['MA'] = temp_df['trade_price'].rolling(rolling_window).mean().shift(1)
                    temp_df['ACTION'] = np.where(temp_df['trade_price'] > temp_df['MA'], 'buy', 'sell')

                    cond_sell = (temp_df['ACTION'] == 'sell') & (temp_df['ACTION'].shift(1) == 'buy')
                    df_sell = temp_df[cond_sell].reset_index(drop=True)
                    df_sell.columns = ['candle_date_time_kst','trade_price', 'MA', 'ACTION']
                    #df_sell['datetime'] = crypto_df['candle_date_time_kst'].iloc[cond_buy].values

                    if not df_sell.empty:
                        last_date_sell = df_sell.tail(1)['candle_date_time_kst'].values[0]
                        last_date_sell = pd.to_datetime(last_date_sell)

                        kst = pytz.timezone('Asia/Seoul')
                        last_date_sell_kst = last_date_sell.tz_localize(kst, ambiguous='NaT')
                        utc_time = datetime.now(timezone.utc)
                        kst_time = utc_time.astimezone(kst)

                        def get_previous_hour(dt):
                            return dt.replace(minute=0, second=0, microsecond=0)

                        previous_hour_time = get_previous_hour(kst_time)

                        if last_date_sell_kst == previous_hour_time:
                            sell_row = pd.DataFrame({
                                'crypto': [crypto],
                                '(n)ma': [rolling_window],
                                'datetime': [last_date_sell],
                                'close': [df_sell.tail(1)['trade_price'].values[0]],
                                'ma': [temp_df['MA'].iloc[-1]]
                            })
                            filter_sell_df = pd.concat([filter_sell_df, sell_row], ignore_index=True)
                            #filter_sell_df = filter_sell_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)          
        
        for crypto in cryptos:
            crypto_df = df[df['market'] == crypto].copy()
            for iloc_range in iloc_ranges:
                for rolling_window in rolling_windows:
                    temp_df = crypto_df[['candle_date_time_kst','trade_price']].iloc[iloc_range:].copy()
                    temp_df['MA'] = temp_df['trade_price'].rolling(rolling_window).mean().shift(1)
                    temp_df['ACTION'] = np.where(temp_df['trade_price'] > temp_df['MA'], 'buy', 'sell')

                    cond_buy = (temp_df['ACTION'] == 'buy') & (temp_df['ACTION'].shift(1) == 'sell')
                    cond_sell = (temp_df['ACTION'] == 'sell') & (temp_df['ACTION'].shift(1) == 'buy')
                    temp_df.iloc[-1, -1] = 'sell'

                    df_buy = temp_df[cond_buy].reset_index()
                    df_buy.columns = ['candle_date_time_kst','trade_price(buy)', 'MA', 'ACTION', 'unkown']
                    df_sell = temp_df[cond_sell].reset_index()
                    df_sell.columns = ['candle_date_time_kst','trade_price(sell)', 'MA', 'ACTION', 'unkown']

                    #filter_return_df = pd.DataFrame(columns=['crypto', '(n)ma', 'day', 'date', 'return(%)'])

                    df_result = pd.concat([df_buy, df_sell], axis=1)
                    df_result['return_rate'] = df_result['trade_price(sell)'] / df_result['trade_price(buy)']
                    df_result = df_result.dropna()

                    kst = pytz.timezone('Asia/Seoul')
                    last_date_sell_kst = last_date_sell.tz_localize(kst, ambiguous='NaT')
                    utc_time = datetime.now(timezone.utc)
                    kst_time = utc_time.astimezone(kst)

                    def get_previous_hour(dt):
                            return dt.replace(minute=0, second=0, microsecond=0)

                    previous_hour_time = get_previous_hour(kst_time)

                    if not df_result.empty:
                        final_return = round((df_result[['return_rate']].cumprod().iloc[-1, -1] - 1) * 100, 1)
                        return_row = pd.DataFrame({
                            'crypto': [crypto],
                            '(n)ma': [rolling_window],
                            'day': [iloc_range],
                            'date': [previous_hour_time],
                            'return(%)': [f"{final_return:.1f}"]
                        })
                        filter_return_df = pd.concat([filter_return_df, return_row], ignore_index=True)
                        #filter_return_df = filter_return_df.astype({'(n)ma': int, 'day': int})
                        #filter_return_df['return(%)'] = filter_return_df['return(%)'].apply(lambda x: round(float(x), 1)).astype(float)
                        #filter_return_df['date'] = filter_return_df['date'].apply(lambda x: x.to_timestamp() if isinstance(x, pd.Period) else x)

        if not filter_buy_df.empty:
            self.log.info(f"{filter_buy_df.to_string(index=False)}")
        else:
            self.log.info("조건을 만족하는 코인 없음") 

        if not filter_sell_df.empty:
            self.log.info(f"{filter_sell_df.to_string(index=False)}")
        else:
            self.log.info("조건을 만족하는 코인 없음") 

        if not filter_return_df.empty:
            self.log.info(f"{filter_return_df.to_string(index=False)}")
        else:
            self.log.info("코드에 문제가 있는 상태") 