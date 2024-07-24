import pandas as pd
from pandasai import SmartDataframe
from pandasai.llm.openai import OpenAI
import os, openai

from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

### Pandas AI###

llm = OpenAI(api_token = api_key)

data = pd.read_excel("C:/Users/blueorange/Desktop/캐롯_리포트.xlsx")

# prompt = f"""
#         다음 데이터를 기간 단위로 데이터를 광고 매체, 디바이스 별로 요약하고
#         Dict 타입으로 반환하시오
#         누락된 데이터는 0 으로 처리한다.
#         """
# smart_dataframe = SmartDataframe(data, config={'llm': llm})
# ai_result = smart_dataframe.chat(prompt)

## CHAT GPT ###
openai.key = api_key
gpt_prompt = f"""
                다음 광고 데이터를 분석하시오.
                {data}
                
                분석 규칙은 아래와 같다
                1. 전체 성과 요약
                2. 광고 매체, 디바이스 별 성과 요약
                3. 기간 비교가 가능한 데이터 셋인 경우 기간 별 비교
            """

response = openai.ChatCompletion.create(
    model='gpt-4o',
    messages=[
        {
            "role": "system",
            "content": """
                        당신은 전문적인 광고 데이터 분석 전문가입니다. 
                        데이터를 다각적으로 분석하고 보고서를 작성합니다.
                    """
        },
        {
            "role": "user",
            "content": gpt_prompt
        }
    ]
)

# Extract the analysis from the response
analysis = response['choices'][0]['message']['content']

# Print the analysis
print("분석 결과:")
print(analysis)
