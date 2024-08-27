from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta

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
        """Converts KRW-BTC to btc"""
        return table_name.split('-')[1].lower()

    def execute(self, context):
        crypto_table_name = self.format_table_name(self.table_name)
        self.log.info(f"Fetching latest trade price from table {crypto_table_name}...")

        conn = self.get_conn()
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}')
        
        query = f"""
        SELECT market, candle_date_time_kst, trade_price
        FROM {crypto_table_name}
        ORDER BY candle_date_time_kst DESC
        LIMIT 120;  
        """
        
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        
        if df.empty:
            self.log.info(f"No data found in table {crypto_table_name}.")
            return
        
        iloc_ranges = [24, 36, 48, 60, 120]
        rolling_windows = [5, 10, 15]
        filter_buy_df = pd.DataFrame(columns=['crypto', '(n)ma', 'datetime', 'close', 'ma'])
        filter_sell_df = pd.DataFrame(columns=['crypto', '(n)ma', 'datetime', 'close', 'ma'])
        filter_return_df = pd.DataFrame(columns=['crypto', '(n)ma', 'day', 'date', 'return(%)'])

        # Separate dataframes for each crypto
        cryptos = df['market'].unique()
        for crypto in cryptos:
            crypto_df = df[df['market'] == crypto].copy()
            for iloc_range in iloc_ranges:
                for rolling_window in rolling_windows:
                    temp_df = crypto_df[['trade_price']].iloc[:iloc_range+1].copy()
                    temp_df['MA'] = temp_df['trade_price'].rolling(rolling_window).mean().shift(1)
                    temp_df['action'] = np.where(temp_df['trade_price'] > temp_df['MA'], 'buy', 'sell')
                    
                    # Buy Conditions
                    cond_buy = (temp_df['action'] == 'buy') & (temp_df['action'].shift(1) == 'sell')
                    df_buy = temp_df[cond_buy].reset_index()
                    df_buy.columns = ['index', 'close(buy)', 'ma', 'action(buy)']
                    df_buy['datetime'] = crypto_df['candle_date_time_kst'].iloc[cond_buy].values

                    if not df_buy.empty:
                        last_date_buy = df_buy.tail(1)['datetime'].values[0]
                        last_date_buy_datetime = pd.to_datetime(last_date_buy)
                        compare_date = datetime.today() - timedelta(1)

                        if last_date_buy_datetime.date() == compare_date.date():
                            buy_row = pd.DataFrame({
                                'crypto': [crypto],
                                '(n)ma': [rolling_window],
                                'datetime': [last_date_buy],
                                'close': [df_buy.tail(1)['close(buy)'].values[0]],
                                'ma': [temp_df['MA'].iloc[-1]]
                            })
                            filter_buy_df = pd.concat([filter_buy_df, buy_row], ignore_index=True)
                            filter_buy_df = filter_buy_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)
                    
                    # Sell Conditions
                    cond_sell = (temp_df['action'] == 'sell') & (temp_df['action'].shift(1) == 'buy')
                    df_sell = temp_df[cond_sell].reset_index()
                    df_sell.columns = ['index', 'close(sell)', 'ma', 'action(sell)']
                    df_sell['datetime'] = crypto_df['candle_date_time_kst'].iloc[cond_sell].values

                    if not df_sell.empty:
                        last_date_sell = df_sell.tail(1)['datetime'].values[0]
                        last_date_sell_datetime = pd.to_datetime(last_date_sell)
                        compare_date = datetime.today() - timedelta(1)

                        if last_date_sell_datetime.date() == compare_date.date():
                            sell_row = pd.DataFrame({
                                'crypto': [crypto],
                                '(n)ma': [rolling_window],
                                'datetime': [last_date_sell],
                                'close': [df_sell.tail(1)['close(sell)'].values[0]],
                                'ma': [temp_df['MA'].iloc[-1]]
                            })
                            filter_sell_df = pd.concat([filter_sell_df, sell_row], ignore_index=True)
                            filter_sell_df = filter_sell_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)
                    
                    # Return Calculation
                    df_buy = temp_df[cond_buy].reset_index()
                    df_sell = temp_df[cond_sell].reset_index()
                    df_result = pd.concat([df_buy, df_sell], axis=1)
                    df_result['return_rate'] = df_result['close(sell)'] / df_result['close(buy)']
                    df_result = df_result.dropna()

                    if not df_result.empty:
                        final_return = round((df_result[['return_rate']].cumprod().iloc[-1, -1] - 1) * 100, 1)
                        return_row = pd.DataFrame({
                            'crypto': [crypto],
                            '(n)ma': [rolling_window],
                            'day': [-iloc_range],
                            'date': [(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")],
                            'return(%)': [f"{final_return:.1f}"]
                        })
                        filter_return_df = pd.concat([filter_return_df, return_row], ignore_index=True)
                        filter_return_df = filter_return_df.astype({'(n)ma': int, 'day': int})
                        filter_return_df['return(%)'] = filter_return_df['return(%)'].apply(lambda x: round(float(x), 1)).astype(float)
                        filter_return_df['date'] = pd.to_datetime(filter_return_df['date'])

        # Prepare data for upload
        buy_data = filter_buy_df.to_dict(orient='records')
        sell_data = filter_sell_df.to_dict(orient='records')
        return_data = filter_return_df.to_dict(orient='records')

        # Send data to DASH
        try:
            payload = {
                'buy_data': buy_data,
                'sell_data': sell_data,
                'return_data': return_data
            }
            response = requests.post(self.dash_api_url, json=payload)
            response.raise_for_status()  # HTTPError 발생시 예외 처리
            self.log.info("Data successfully uploaded to DASH.")
        except requests.RequestException as e:
            self.log.error(f"Failed to upload data to DASH: {e}")