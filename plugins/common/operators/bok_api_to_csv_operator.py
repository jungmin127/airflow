from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
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

        # CSV 파일로 저장
        file_path = f"{self.path}/{self.file_name}"
        df.to_csv(file_path, encoding='utf-8', index=False)
        self.log.info(f"데이터프레임을 CSV 파일로 저장: {file_path}")

    def _call_api(self, startdate, enddate):
        connection = BaseHook.get_connection(self.http_conn_id)
        base_url = f'https://ecos.bok.or.kr/api/StatisticSearch/{{key}}/json/kr/1/500/802Y001/D/{startdate}/{enddate}/0001000'

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        url = base_url.format(key=connection.password)  # API key를 적용한 URL 생성
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            contents = json.loads(response.text)
            key_nm = list(contents.keys())[0]
            
            if key_nm in contents and 'row' in contents[key_nm]:
                return contents[key_nm]['row']
            else:
                self.log.error(f"API 응답에서 'row' 데이터를 찾을 수 없음")
                return []
        else:
            self.log.error(f"API 호출 실패. Status code: {response.status_code}")
            return []