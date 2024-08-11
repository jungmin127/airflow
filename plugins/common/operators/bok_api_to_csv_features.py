from airflow.models.baseoperator import BaseOperator
from airflow.models import Variable
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

class BokApiToDataFrameOperator(BaseOperator):
    template_fields = ('path', 'file_name_template')

    def __init__(self, path, file_name_template, indicators, data_type_code, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'bok_api'
        self.path = path
        self.file_name_template = file_name_template
        self.indicators = indicators  # [{'code': '802Y001', 'name': 'indicator1'}, {'code': '0001000', 'name': 'indicator2'}]
        self.data_type_code = data_type_code  # '0001000' 또는 다른 데이터 타입 코드

    def execute(self, context):
        execution_date = context['execution_date']
        days_back = 4
        startdate = (execution_date - timedelta(days=days_back)).strftime('%Y%m%d')
        enddate = execution_date.strftime('%Y%m%d')

        for indicator in self.indicators:
            code = indicator['code']
            name = indicator['name']
            kospi_data = self._call_api(startdate, enddate, code)

            df = pd.DataFrame(kospi_data)
            
            directory_path = os.path.join(self.path, name)
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)

            file_path = os.path.join(directory_path, f"{self.file_name_template.format(name=name)}")
            df.to_csv(file_path, encoding='utf-8', index=False)
            self.log.info(f"데이터프레임을 CSV 파일로 저장: {file_path}")

    def _call_api(self, startdate, enddate, indicator_code):
        api_key = Variable.get('apikey_openapi_bok')
        base_url = f'https://ecos.bok.or.kr/api/StatisticSearch/{api_key}/json/kr/1/500/{indicator_code}/D/{startdate}/{enddate}/{self.data_type_code}'

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        response = requests.get(base_url, headers=headers)

        self.log.info(f"API 응답 내용: {response.text}")

        if response.status_code == 200:
            contents = json.loads(response.text)
            self.log.info(f"API 응답 데이터: {contents}")
            key_nm = list(contents.keys())[0]
            
            if key_nm in contents and 'row' in contents[key_nm]:
                return contents[key_nm]['row']
            else:
                self.log.error(f"API 응답에서 'row' 데이터를 찾을 수 없음")
                return []
        else:
            self.log.error(f"API 호출 실패. Status code: {response.status_code}")
            return []
