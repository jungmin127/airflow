# import dash
# import dash_table
# import pandas as pd
# import psycopg2
# from dash import dcc, html
# from dash.dependencies import Input, Output

# # Dash 앱 초기화
# app = dash.Dash(__name__)

# # PostgreSQL 데이터베이스에 연결하여 데이터 가져오기
# def fetch_data():
#     conn = psycopg2.connect(
#         host='your_host',
#         database='your_db',
#         user='your_user',
#         password='your_password'
#     )
#     query = """
#     SELECT * FROM (
#         SELECT * FROM buy_conditions
#         UNION ALL
#         SELECT * FROM sell_conditions
#         UNION ALL
#         SELECT * FROM return_conditions
#     ) AS all_data
#     WHERE datetime >= CURRENT_DATE - INTERVAL '1 day'
#     """
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df

# # 앱 레이아웃 정의
# app.layout = html.Div([
#     html.H1('Crypto Data Dashboard'),
#     dcc.Interval(
#         id='interval-component',
#         interval=1*60*1000,  # 1분마다 새로고침
#         n_intervals=0
#     ),
#     dash_table.DataTable(
#         id='crypto-table',
#         columns=[{'name': i, 'id': i} for i in fetch_data().columns],
#         style_table={'overflowX': 'auto'},
#     )
# ])

# # 콜백 정의: 테이블 데이터 업데이트
# @app.callback(
#     Output('crypto-table', 'data'),
#     Input('interval-component', 'n_intervals')
# )
# def update_table(n):
#     df = fetch_data()
#     return df.to_dict('records')

# # 앱 실행
# if __name__ == '__main__':
#     app.run_server(debug=True)
