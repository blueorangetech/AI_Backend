import pandas as pd
import os, openai, requests

from utils import chunk_dict, dataframe_chunk, timestamp_to_str
from gpt_models.gpt_option import interim_report, data_summary
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
openai.key = api_key

data = pd.read_excel("C:/Users/blueorange/Desktop/캐롯_리포트.xlsx")
data.fillna(0, inplace=True)
data = timestamp_to_str(data)
size = 10

split_data = chunk_dict(data, size)

split_response = []
for idx, chunk in enumerate(split_data):

    req = f"다음 데이터의 기간 단위로, 매체 및 디바이스 별 노출, 클릭, 전환을 구해줘 \n 데이터: {chunk}"
    message = [{"role": "user", "content": req}]

    response = interim_report(idx, message, chunk)

    print(f"##### {idx} 데이터 #####")
    print(sum(chunk["노출"]), sum(chunk["클릭"]))
    print(response)
    if idx == 0:
        break
#     split_response.append(response)

# for idx, s in enumerate(split_response):
#     print(idx)
#     print(s)

# result = '\n'.join(split_response)

# final_req = f"다음 개별 보고서를 종합하여 최종 보고서를 작성하시오 \n데이터: {result}"
# summary = data_summary(final_req)
# print("######## Result ########")
# print(summary)