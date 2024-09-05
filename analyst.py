import pandas as pd
from gpt_models.gpt_option import interim_report
from gpt_models.data_analyst import data_analyst_description
from gpt_models.trend_finder import trend_finder_description
from gpt_models.media_analyst import media_analyst_description

import os, openai

## CHAT GPT ###
def analyst(file_path):
        agent = data_analyst_description()

        req = f'''
                7월의 성과를 구하고 6월 대비 변동률을 구해라
                데이터 경로:{file_path}
                '''
        msg = {"role": "user", "content": req}

        response = interim_report(msg, agent)
        return response

def media_analyst(file_path):
        agent = media_analyst_description()

        req = f'''
                다음 데이터를 분석하시오
                데이터 경로:{file_path}
                '''
        msg = {"role": "user", "content": req}

        response = interim_report(msg, agent)
        return response

def trend_finder(file_path):
        agent = trend_finder_description()

        req = f'''
                다음 데이터를 분석하시오
                데이터 경로 {file_path}
                '''
        
        msg = {"role": "user", "content": req}
        response = interim_report(msg, agent)
        return response

if __name__ == "__main__":
        file_path = "C:/Users/blueorange/Desktop/캐롯_리포트.xlsx"
        result = analyst(file_path)
        print(result)