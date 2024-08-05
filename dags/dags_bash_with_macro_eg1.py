from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_bash_with_macro_eg1',
    schedule='10 0 L * *', #매월 말일에 실행되는 Dag
    start_date=pendulum.datetime(2024, 8, 5, tz='Asia/Seoul'),
    catchup=False
) as dag:
    # 구하려는것 -> START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id = 'bash_task_1',
        env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"' 
    )

    #strat_date 에 .in_timezone 을 붙이는 이유 -> 기본은 UTC 이기 때문에 수정필요
    #end_date 에서 하루를 뺴기 위해 relativedelta 추가
    # | ds 를 붙이면 yyyy-mm-dd 형식으로 나옴