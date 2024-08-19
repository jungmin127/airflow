# from airflow import DAG
# import pendulum
# from airflow.operators.python import PythonOperator
# import pandas as pd
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import mean_squared_error
# from common.hooks.custom_crypto_postgres_hook import CustomCryptoPostgresHook

# def query_data_from_postgres(postgres_conn_id, query, **kwargs):
#     hook = CustomCryptoPostgresHook(postgres_conn_id=postgres_conn_id)
#     conn = hook.get_conn()
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df

# def ml_model(**kwargs):
#     ti = kwargs['ti']
#     postgres_conn_id = kwargs['postgres_conn_id']

#     query = "SELECT candle_date_time_kst, opening_price, high_price, low_price, trade_price, candle_acc_trade_price\
#             FROM btc \
#             GROUP BY 1,2,3,4,5,6"
#     df = query_data_from_postgres(postgres_conn_id, query)
    
#     df['candle_date_time_kst'] = pd.to_datetime(df['candle_date_time_kst'], format='ISO8601')
#     df['year'] = df['candle_date_time_kst'].dt.year
#     df['month'] = df['candle_date_time_kst'].dt.month
#     df['day'] = df['candle_date_time_kst'].dt.day
#     df['hour'] = df['candle_date_time_kst'].dt.hour

#     X = df.drop(columns=['trade_price','candle_date_time_kst'])
#     y = df['trade_price']

#     X_tr, X_val, y_tr, y_val = train_test_split(X, y, test_size=0.2, random_state=1234)

#     model = RandomForestRegressor(n_estimators=100, random_state=1234)
#     model.fit(X_tr, y_tr)

#     pred = model.predict(X_val)
#     score = mean_squared_error(y_val, pred)
#     print(f'Model Score(MSE): {score:.2f}')

# with DAG(
#     dag_id='dags_btc_ml',
#     start_date=pendulum.datetime(2024, 8, 18, tz='Asia/Seoul'),
#     schedule='3 * * * *',
#     catchup=False
# ) as dag:

#     btc_ml_model_task = PythonOperator(
#         task_id='btc_ml_model_task',
#         python_callable=ml_model,
#         op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom'}
#     )
