from airflow.models.baseoperator import BaseOperator
import pandas as pd
import requests
import json
import os
from datetime import datetime

class CryptoToCSVOperator(BaseOperator):
    template_fields = ('path', 'file_name', 'market')

    def __init__(self, path, file_name, market, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.file_name = file_name
        self.market = market

    def execute(self, context):
        data = self._call_api()
        if data:
            df = pd.DataFrame(data)
            df['candle_date_time_kst'] = pd.to_datetime(df['candle_date_time_kst'])
            df = df.drop_duplicates(subset=['candle_date_time_kst'])
            
            directory_path = os.path.join(self.path)
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)
            file_path = os.path.join(directory_path, self.file_name)

            if os.path.exists(file_path):
                existing_df = pd.read_csv(file_path, encoding='utf-8')
                existing_df['candle_date_time_kst'] = pd.to_datetime(existing_df['candle_date_time_kst'])
                combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['candle_date_time_kst'])
            else:
                combined_df = df

            combined_df.to_csv(file_path, encoding='utf-8', index=False)
            self.log.info(f"데이터프레임을 CSV 로 저장: {file_path}")
        else:
            self.log.error('API 호출 결과 없음')

    def _call_api(self):
        base_url = f'https://api.upbit.com/v1/candles/minutes/60?market={self.market}&count=200'

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        response = requests.get(base_url, headers=headers)
        self.log.info(f"API 응답: {response.text}")

        if response.status_code == 200:
            contents = response.json()
            self.log.info(f"API 응답 데이터: {contents}")
            return contents
        else:
            self.log.error(f"API 호출 실패 코드: {response.status_code}")
            return []