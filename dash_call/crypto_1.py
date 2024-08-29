import dash
from dash import dash_table
import pandas as pd
import psycopg2
from dash import dcc, html
from dash.dependencies import Input, Output

app = dash.Dash(__name__)

def fetch_data():
    conn = psycopg2.connect(
        host='localhost',
        database='strawfev',
        user='strawfev',
        password='strawfev'
    )
    query1 = "SELECT * FROM buy_conditions"
    query2 = "SELECT * FROM sell_conditions"
    query3 = "SELECT * FROM return_conditions"
    df1 = pd.read_sql(query1, conn)
    df2 = pd.read_sql(query2, conn)
    df3 = pd.read_sql(query3, conn)
    conn.close()
    return df1, df2, df3

app.layout = html.Div([
    html.H1('현재 기준 Buy/Sell/Return Dashboard', style={'textAlign': 'center', 'color': '#2c3e50'}),
    dcc.Interval(
        id='interval-component',
        interval=30*60*1000,
        n_intervals=0
    ),
    dcc.Tabs(
        id='tabs',
        value='tab-1',
        children=[
            dcc.Tab(
                label='Buy Conditions',
                value='tab-1',
                style={
                    'padding': '10px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#f4f4f4',
                    'color': '#3498db',
                    'border': '1px solid #ddd',
                    'borderRadius': '4px'
                },
                selected_style={
                    'backgroundColor': '#3498db',
                    'color': '#fff',
                    'border': '1px solid #3498db',
                    'borderRadius': '4px'
                }
            ),
            dcc.Tab(
                label='Sell Conditions',
                value='tab-2',
                style={
                    'padding': '10px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#f4f4f4',
                    'color': '#3498db',
                    'border': '1px solid #ddd',
                    'borderRadius': '4px'
                },
                selected_style={
                    'backgroundColor': '#3498db',
                    'color': '#fff',
                    'border': '1px solid #3498db',
                    'borderRadius': '4px'
                }
            ),
            dcc.Tab(
                label='Return Conditions',
                value='tab-3',
                style={
                    'padding': '10px',
                    'fontWeight': 'bold',
                    'backgroundColor': '#f4f4f4',
                    'color': '#3498db',
                    'border': '1px solid #ddd',
                    'borderRadius': '4px'
                },
                selected_style={
                    'backgroundColor': '#3498db',
                    'color': '#fff',
                    'border': '1px solid #3498db',
                    'borderRadius': '4px'
                }
            )
        ],
        style={
            'border': '1px solid #ddd',
            'borderRadius': '4px',
            'marginBottom': '10px'
        }
    ),
    html.Div(id='tabs-content')
])

@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs', 'value'),
    Input('interval-component', 'n_intervals')
)
def update_tab(tab_name, n):
    df1, df2, df3 = fetch_data()
    if tab_name == 'tab-1':
        df = df1
    elif tab_name == 'tab-2':
        df = df2
    elif tab_name == 'tab-3':
        df = df3
    else:
        df = pd.DataFrame()
    
    return dash_table.DataTable(
        columns=[{'name': i, 'id': i} for i in df.columns],
        data=df.to_dict('records'),
        style_table={
            'overflowX': 'auto',
            'maxWidth': '100%',
            'margin': '0 auto',
            'backgroundColor': '#fff',
            'borderRadius': '8px',
            'boxShadow': '0 4px 8px rgba(0,0,0,0.1)'
        },
        style_header={
            'backgroundColor': '#3498db',
            'color': '#fff',
            'fontWeight': 'bold'
        },
        style_cell={
            'padding': '10px'
        }
    )

if __name__ == '__main__':
    app.run_server(debug=True)