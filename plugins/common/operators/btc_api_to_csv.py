from airflow.models.baseoperator import BaseOperator
import pandas as pd
import requests
import json
import os
from datetime import datetime

class BTCtoCSVOperator(BaseOperator):
    template_fields = ('path', 'file_name_template')

    def __init__(self, path, file_name_template, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.file_name_template = file_name_template

    def execute(self, context):
        btc_data = self._call_api()
        if btc_data:
            df = pd.DataFrame(btc_data)
            
            directory_path = os.path.join(self.path)
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)

            file_path = os.path.join(directory_path, self.file_name_template.format(date=datetime.now().strftime('%Y-%m-%d')))
            df.to_csv(file_path, encoding='utf-8', index=False)
            self.log.info(f"데이터프레임을 CSV 파일로 저장: {file_path}")
        else:
            self.log.error('API 호출 결과가 없음')

    def _call_api(self):
        base_url = f'https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&count=200'

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        response = requests.get(base_url, headers=headers)

        self.log.info(f"API 응답 내용: {response.text}")

        if response.status_code == 200:
            contents = response.jason()
            self.log.info(f"API 응답 데이터: {contents}")
            return contents
        else:
            self.log.error(f"API 호출 실패. Status code: {response.status_code}")
            return []