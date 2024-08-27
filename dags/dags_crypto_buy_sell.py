import pandas as pd
import pyupbit
import numpy as np
import requests
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
pd.options.display.float_format = '{:.1f}'.format

def define_crypto(crypto, to_date, count):
    try:
        df = pyupbit.get_ohlcv(crypto, interval="day", to=to_date, count=count, period=0.1)
        if df is None:
            raise ValueError(f"No data returned for {crypto}")
        df = df.reset_index()
        df = df.rename(columns={'index': 'date'})
        df['date'] = df['date'].dt.to_period(freq='D')
        df = df.set_index('date')
        return df
    except Exception as e:
        print(f"Error fetching data for {crypto}: {e}")
        return None

dataframes = {}
crypto_names = []

cols = ['KRW-SOL', 'KRW-XRP', 'KRW-ETH', 'KRW-TRX', 'KRW-BTC', 'KRW-LINK',
        'KRW-HBAR', 'KRW-ETC', 'KRW-DOGE', 'KRW-ALGO', 'KRW-BSV', 'KRW-EOS',
        'KRW-DOT', 'KRW-FLOW', 'KRW-ADA', 'KRW-SC', 'KRW-SAND', 'KRW-UPP', 'KRW-QTUM']

for crypto in cols:
    crypto_name = crypto.split('-')[1]
    df = define_crypto(crypto, datetime.today().strftime("%Y%m%d"), 365)
    if df is not None:
        dataframes[crypto_name] = df
        crypto_names.append(crypto_name)

url = "https://api.upbit.com/v1/market/all?isDetails=true"
headers = {"accept": "application/json"}

res = requests.get(url, headers=headers)
en_name = [content['market'] for content in res.json()]
kr_name = [content['korean_name'] for content in res.json()]

name = pd.DataFrame({'crypto': en_name, 'name': kr_name})
name['krw'] = name['crypto'].str.contains('KRW')
kr_name = name[name['krw'] == 1].reset_index(drop=True)
kr_name = kr_name.drop(columns=['krw'])
kr_name['crypto'] = kr_name['crypto'].str.replace("KRW-", "")

pd.options.display.float_format = '{:.1f}'.format
iloc_ranges = [-30, -50, -100, -200, -365]
rolling_windows = [5, 10, 15]

filter_buy_df = pd.DataFrame(columns=['crypto', '(n)ma', 'date', 'close', 'ma'])

for crypto_name in crypto_names:
    for iloc_range in iloc_ranges:
        for rolling_window in rolling_windows:
            df = dataframes[crypto_name][['close']].iloc[iloc_range:, :].copy()
            df['MA'] = df['close'].rolling(rolling_window).mean().shift(1)
            df['action'] = np.where(df['close'] > df['MA'], 'buy', 'sell')
            cond_buy = (df['action'] == 'buy') & (df['action'].shift(1) == 'sell')
            df_buy = df[cond_buy].reset_index()
            df_buy.columns = ['date(buy)', 'close(buy)', 'ma', 'action(buy)']

            if not df_buy.empty:
                last_date_buy = df_buy.tail(1)['date(buy)'].values[0]
                last_date_buy_datetime = last_date_buy.to_timestamp()
                compare_date = datetime.today() - timedelta(1)

                if last_date_buy_datetime.date() == compare_date.date():
                    buy_row = pd.DataFrame({
                        'crypto': [crypto_name],
                        '(n)ma': [rolling_window],
                        'date': [last_date_buy],
                        'close': [df_buy.tail(1)['close(buy)'].values[0]],
                        'ma': [df['close'].rolling(rolling_window).mean().iloc[-1]]
                    })
                    filter_buy_df = pd.concat([filter_buy_df, buy_row], ignore_index=True)
                    filter_buy_df = filter_buy_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)
                    
filter_sell_df = pd.DataFrame(columns=['crypto', '(n)ma', 'date', 'close', 'ma'])

for crypto_name in crypto_names:
    for iloc_range in iloc_ranges:
        for rolling_window in rolling_windows:
            df = dataframes[crypto_name][['close']].iloc[iloc_range:, :].copy()
            df['MA'] = df['close'].rolling(rolling_window).mean().shift(1)
            df['action'] = np.where(df['close'] > df['MA'], 'buy', 'sell')
            cond_sell = (df['action'] == 'sell') & (df['action'].shift(1) == 'buy')
            df_sell = df[cond_sell].reset_index()
            df_sell.columns = ['date(sell)', 'close(sell)', 'ma', 'action(sell)']

            if not df_sell.empty:
                last_date_sell = df_sell.tail(1)['date(sell)'].values[0]
                last_date_sell_datetime = last_date_sell.to_timestamp()
                compare_date = datetime.today() - timedelta(1)

                if last_date_sell_datetime.date() == compare_date.date():
                    sell_row = pd.DataFrame({
                        'crypto': [crypto_name],
                        '(n)ma': [rolling_window],
                        'date': [last_date_sell],
                        'close': [df_sell.tail(1)['close(sell)'].values[0]],
                        'ma': [df['close'].rolling(rolling_window).mean().iloc[-1]]
                    })
                    filter_sell_df = pd.concat([filter_sell_df, sell_row], ignore_index=True)
                    filter_sell_df = filter_sell_df.drop_duplicates(['crypto', '(n)ma'], keep='last').reset_index(drop=True)
                    
filter_return_df = pd.DataFrame(columns=['crypto', '(n)ma', 'day', 'date', 'return(%)'])

for crypto_name in crypto_names:
    for iloc_range in iloc_ranges:
        for rolling_window in rolling_windows:
            df = dataframes[crypto_name][['close']].iloc[iloc_range:, :].copy()
            df['MA'] = df['close'].rolling(rolling_window).mean().shift(1)
            df['action'] = np.where(df['close'] > df['MA'], 'buy', 'sell')
            cond_buy = (df['action'] == 'buy') & (df['action'].shift(1) == 'sell')
            cond_sell = (df['action'] == 'sell') & (df['action'].shift(1) == 'buy')
            df.iloc[-1, -1] = 'sell'

            df_buy = df[cond_buy].reset_index()
            df_buy.columns = ['date(buy)', 'close(buy)', 'ma', 'action(buy)']
            df_sell = df[cond_sell].reset_index()
            df_sell.columns = ['date(sell)', 'close(sell)', 'ma', 'action(sell)']

            df_result = pd.concat([df_buy, df_sell], axis=1)
            df_result['return_rate'] = df_result['close(sell)'] / df_result['close(buy)']
            df_result = df_result.dropna()

            if not df_result.empty:
                final_return = round((df_result[['return_rate']].cumprod().iloc[-1, -1] - 1) * 100, 1)
                return_row = pd.DataFrame({
                    'crypto': [crypto_name],
                    '(n)ma': [rolling_window],
                    'day': [-iloc_range],
                    'date': [(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")],
                    'return(%)': [f"{final_return:.1f}"]
                })
                filter_return_df = pd.concat([filter_return_df, return_row], ignore_index=True)
                filter_return_df = filter_return_df.astype({'(n)ma': int, 'day': int})
                filter_return_df['return(%)'] = filter_return_df['return(%)'].apply(lambda x: round(float(x), 1)).astype(float)
                #filter_return_df['date'] = pd.to_datetime(filter_return_df['date'])
                filter_return_df['date'] = filter_return_df['date'].apply(lambda x: x.to_timestamp() if isinstance(x, pd.Period) else x)

filter_buy_df = pd.merge(filter_buy_df, kr_name, how='left', on='crypto')
filter_buy_df = filter_buy_df[['date', 'crypto', 'name', '(n)ma', 'close', 'ma']]

filter_sell_df = pd.merge(filter_sell_df, kr_name, how='left', on='crypto')
filter_sell_df = filter_sell_df[['date', 'crypto', 'name', '(n)ma', 'close', 'ma']]

filter_return_df = pd.merge(filter_return_df, kr_name, how='left', on='crypto')
filter_return_df = filter_return_df[['date', 'crypto', 'name', '(n)ma', 'day', 'return(%)']]

#filter_buy_df['date'] = pd.to_datetime(filter_buy_df['date'])
filter_buy_df['date'] = filter_buy_df['date'].apply(lambda x: x.to_timestamp() if isinstance(x, pd.Period) else x)
filter_buy_df['(n)ma'] = filter_buy_df['(n)ma'].astype(int)
filter_buy_df['close'] = filter_buy_df['close'].astype(float)
filter_buy_df['ma'] = filter_buy_df['ma'].astype(float) 

#filter_sell_df['date'] = pd.to_datetime(filter_sell_df['date'])
filter_sell_df['date'] = filter_sell_df['date'].apply(lambda x: x.to_timestamp() if isinstance(x, pd.Period) else x)
filter_sell_df['(n)ma'] = filter_sell_df['(n)ma'].astype(int)
filter_sell_df['close'] = filter_sell_df['close'].astype(float)
filter_sell_df['ma'] = filter_sell_df['ma'].astype(float)