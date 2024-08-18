# from airflow import DAG
# import pendulum
# import pandas as pd
# from airflow.operators.python import PythonOperator
# from airflow.providers import PostgresHook
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.model_selection import train_test_split
# from sklearn.metrics import accuracy_score

# # DAG 정의
# with DAG(
#         dag_id='process_postgres_data',
#         start_date=pendulum.datetime(2024, 8, 10, tz='Asia/Seoul'),
#         schedule='@daily',  # 매일 실행
#         catchup=False
# ) as dag:

#     def query_and_process_data(**kwargs):
#         # PostgreSQL Hook 생성
#         postgres_hook = PostgresHook(postgres_conn_id='conn-db-postgres-custom')
#         conn = postgres_hook.get_conn()
#         cursor = conn.cursor()

#         # 쿼리 실행
#         query = """
#         SELECT * FROM bok_kospi;
#         """
#         cursor.execute(query)
#         rows = cursor.fetchall()
        
#         # 데이터프레임으로 변환
#         columns = [desc[0] for desc in cursor.description]
#         df = pd.DataFrame(rows, columns=columns)
        
#         # 머신러닝 모델 학습 (예시)
#         X = df.drop('target_column', axis=1)  # 'target_column'은 예시로 사용된 목표 열
#         y = df['target_column']
        
#         # 데이터 분할
#         X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
#         # 모델 학습
#         model = RandomForestClassifier()
#         model.fit(X_train, y_train)
        
#         # 예측 및 평가
#         y_pred = model.predict(X_test)
#         accuracy = accuracy_score(y_test, y_pred)
        
#         print(f'Model Accuracy: {accuracy:.2f}')

#     # PythonOperator 태스크 정의
#     process_data_task = PythonOperator(
#         task_id='query_and_process_data',
#         python_callable=query_and_process_data
#     )