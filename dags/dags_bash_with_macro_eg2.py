from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_bash_with_macro_eg1',
    schedule='10 0 * * 6#2',  #매월 둘째주 토요일에 도는 Dag
    start_date=pendulum.datetime(2024, 6, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    # 구하려는것-> START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2',
        env={'START_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}'
        },
        bash_commnad='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"' 
    )