from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
import pandas as pd
import requests
import json
from datetime import datetime, timedelta

class BokKospiToDataFrameOperator(BaseOperator):
    template_fields = ('path', 'file_name')

    def __init__(self, path, file_name, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'bok_api'
        self.path = path
        self.file_name = file_name

    def execute(self, context):
        # 현재 날짜 구하기
        today = datetime.now().strftime('%Y%m%d')

        # API 호출
        kospi_data = self._call_api(today, today)

        # 데이터프레임 생성
        df = pd.DataFrame(kospi_data)
        
        import os
        # 디렉토리 경로 설정
        directory_path = '/opt/airflow/files/kospi_value/'
        # 디렉토리가 없는 경우 생성
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # CSV 파일로 저장
        file_path = f"{self.path}/{self.file_name}"
        df.to_csv(file_path, encoding='utf-8', index=False)
        self.log.info(f"데이터프레임을 CSV 파일로 저장: {file_path}")

    def _call_api(self, startdate, enddate):
        api_key = Variable.get('apikey_openapi_bok')  # Airflow Variable의 이름
        base_url = f'https://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/kr/1/500/802Y001/D/{startdate}/{enddate}/0001000'

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        response = requests.get(base_url, headers=headers)

        self.log.info(f"API 응답 내용: {response.text}")  # 응답 내용을 로깅

        if response.status_code == 200:
            contents = json.loads(response.text)
            self.log.info(f"API 응답 데이터: {contents}")  # 응답 데이터 구조 로깅
            key_nm = list(contents.keys())[0]
            
            if key_nm in contents and 'row' in contents[key_nm]:
                return contents[key_nm]['row']
            else:
                self.log.error(f"API 응답에서 'row' 데이터를 찾을 수 없음")
                return []
        else:
            self.log.error(f"API 호출 실패. Status code: {response.status_code}")
            return []