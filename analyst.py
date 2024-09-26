from gpt_models.gpt_option import interim_report
from gpt_models.trend_finder import makerter_description

from tools.group_data import group_data
from tools.keyword_similartiy import check_similarity

import pandas as pd
import os, openai, json, time

## CHAT GPT ###
def analyst(file_path):
	data = pd.read_excel(file_path)
	
	fields = ["노출", "클릭", "광고비", "PA청약"] # 수정 - 하드코딩
	data["기준"] = data["날짜"].dt.month # 수정 - 하드코딩

	arguments = {"data": data, "standard": ["기준"],
				"fields": [], "sum_fields" : fields, "limit": None}

	total_data = group_data(arguments)
	agent = makerter_description()

	req = f"다음 데이터를 요약하시오\n{total_data}"
	msg = {"role": "user", "content": req}

	response = interim_report(msg)
	return response

def media_analyst(file_path):
	data = pd.read_excel(file_path)
	
	fields = ["노출", "클릭", "광고비", "PA청약"] # 수정 - 하드코딩
	fields = ["노출", ["CTR", "클릭", "노출", 100]] # 수정 - 하드코딩
	data["기준"] = data["날짜"].dt.month # 수정 - 하드코딩
	
	total_data = []

	for field in fields:
		arguments = {"data": data, "standard": ["기준"],
				"fields": ["상품구분(매체)", "디바이스"], "sum_fields" : field, "limit": 1}
		
		total_data.append(group_data(arguments))
	
	req = f"다음 데이터를 요약하시오\n{total_data}"
			
	msg = {"role": "user", "content": req}
	agent = makerter_description()
	response = interim_report(msg)

	return response

def keyword_analyst(file_path):

	# 41 seconds
	data = pd.read_excel(file_path)

	# 키워드 유사도 기준 그룹화
	keywords = data["키워드"].str.lower().drop_duplicates(keep='first')
	unique_keywords = keywords.to_list()

	groups, groups_map = check_similarity(unique_keywords) # 90 seconds
	data["키워드그룹"] = data["키워드"].map(groups_map)

	fields = ["노출수", "클릭수", "총비용", "PA 청약"] # 수정 - 하드코딩
	data["기준"] = data["기간"].dt.month # 수정 - 하드코딩
	
	arguments = {"data": data, "standard": ["기준"],
			  "fields": ["키워드그룹"], "sum_fields" : "클릭수", "limit": 1}
	
	total_data = group_data(arguments)

	group_info = [f"키워드 그룹 {i}: {groups[int(i)]}" for i in total_data["키워드그룹"]]

	## 매체 분석
	data_chunks = []

	for keyword in total_data["키워드그룹"]:
		keyword_data = data[data["키워드그룹"] == keyword]
		arguments = {"data": keyword_data, "standard": ["기준"],
			   "fields": ["매체구분", "키워드그룹"], "sum_fields" : "클릭수", "limit": 1}
		
		data_chunks.append(group_data(arguments))

	media_data = pd.concat(data_chunks)
	req = f"다음 데이터를 분석하시오\n{total_data}\n{media_data}\{group_info}"
			
	msg = {"role": "user", "content": req}
	agent = makerter_description()
	response = interim_report(msg, agent)

	return response

if __name__ == "__main__":
	file_path = "C:/Users/blueorange/Desktop/캐롯_리포트.xlsx"
	result = media_analyst(file_path)
	print(result)