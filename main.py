import pandas as pd
from pandasai import SmartDataframe, PandasAI
from pandasai.llm.openai import OpenAI
from gpt_models.gpt_option import interim_report
import os, openai

from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

### Pandas AI###

llm = OpenAI(api_token = api_key)

data = pd.read_excel("C:/Users/blueorange/Desktop/캐롯_리포트.xlsx")

prompt = f"""
        다음 데이터는 광고 성과 데이터다
        일자 단위로 광고 매체 및 디바이스 별 성과를 알려줘
        """
smart_dataframe = SmartDataframe(data, config={'llm': llm})
ai_result = smart_dataframe.chat(prompt)
print(ai_result)

## CHAT GPT ###
# print(data)
openai.key = api_key

req = f'7/18 일 기준으로 광고 매체와 디바이스 별 성과를 요약하고 7/17일 대비 증감률을 알려줘 \n데이터: {ai_result}'
message = [{"role": "user", "content": req}]
response = interim_report(0, message, ai_result)

# Extract the analysis from the response

# Print the analysis
print("분석 결과:")
print(response)
