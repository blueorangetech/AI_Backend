from gpt_models.gpt_option import interim_report
from gpt_models.keyword_analyst import makerter_description
from gpt_models.general_analyst import general_analyst_description

from tools.group_data import group_data
from tools.keyword_similartiy import check_similarity

import pandas as pd
import time

## CHAT GPT ###
def analyst(pre_result):
	print("Processing Total Analyst")
	fields = ["노출", "클릭", "광고비", "PA청약"] # 수정 - 하드코딩

	arguments = {"data": pre_result, "standard": ["기준"],
				"fields": [], "sum_fields" : fields, "limit": None}

	total_data = group_data(arguments)
	agent = general_analyst_description()
	
	req = f"다음 데이터를 분석 한 후 요약하라.\n{total_data}"
	msg = {"role": "user", "content": req}

	response = interim_report(msg, agent)
	result = response.replace("```html", "").replace("```", "")	
	
	return result

def media_analyst(pre_result):
	print("Processing Media Analyst")
	
	fields = ["노출", "클릭", "광고비", "PA청약"] # 수정 - 하드코딩
	
	fields = ["PA청약", 
		   ["CPA", "광고비", "PA청약", 1],
		   "광고비",
		   ["CPC", "광고비", "클릭", 1],
		   ["CTR", "클릭", "노출", 100], 
		   "노출", 
		   "클릭", 
		   ["CVR", "PA청약", "클릭", 100],
		   ] # 수정 - 하드코딩

	total_data = []

	for field in fields:
		arguments = {"data": pre_result, "standard": ["기준"],
				"fields": ["상품구분(매체)", "디바이스"], "sum_fields" : field, "limit": 1}
		
		total_data.append(group_data(arguments))

	agent = general_analyst_description()
	req = f"다음 데이터를 지표 별로 테이블을 분리하여 요약하시오. \n{total_data}"
			
	msg = {"role": "user", "content": req}
	
	response = interim_report(msg, agent)
	result = response.replace("```html", "").replace("```", "")
	return result

def keyword_analyst(pre_result):
	print("Processing Keyword Analyst")

	# 키워드 유사도 기준 그룹화
	keywords = pre_result["키워드"].str.lower().drop_duplicates(keep='first')
	unique_keywords = keywords.to_list()

	groups, groups_map = check_similarity(unique_keywords) # 90 seconds
	pre_result["키워드그룹"] = pre_result["키워드"].map(groups_map)

	fields = ["청약", 
		   ["CPA", "광고비", "청약", 1],
		   "광고비",
		   ["CPC", "광고비", "클릭", 1],
		   "노출", 
		   "클릭",
		   ] # 수정 - 하드코딩
	
	## 매체 분석
	response = {}
	agent = general_analyst_description()

	for field in fields:
		start = time.time()
		# 데이터 1차 분석 - 등락 폭이 가장 심한 키워드 그룹

		arguments = {"data": pre_result, "standard": ["기준"],
				"fields": ["키워드그룹"], "sum_fields" : field, "limit": 1}
		
		total_data = group_data(arguments)

		group_info = [f"키워드 그룹 {i}: {groups[int(i)]}" for i in total_data["키워드그룹"]]

		data_chunks = []

		# 데이터 2차 분석 - 키워드 그룹 기준, 
		for keyword in total_data["키워드그룹"]:
			keyword_data = pre_result[pre_result["키워드그룹"] == keyword]
			arguments = {"data": keyword_data, "standard": ["기준"],
				"fields": ["매체구분", "키워드그룹"], "sum_fields" : field, "limit": 1}
			
			data_chunks.append(group_data(arguments))

		media_data = pd.concat(data_chunks)

		req = f"""다음 데이터를 분석하시오 키워드 그룹은 번호 대신 예시 키워드를 사용한다.\n{total_data}\n{media_data}\{group_info}"""
				
		msg = {"role": "user", "content": req}

		result = interim_report(msg, agent)
		
		if isinstance(field, str):
			response[field] = result.replace("```html", "").replace("```", "")	
		
		else:
			response[field[0]] = result.replace("```html", "").replace("```", "")	

		end = time.time()
		print(f'Processing time: {end - start}')

	return response

if __name__ == "__main__":
	file_path = "C:/Users/blueorange/Desktop/캐롯_리포트.xlsx"
	result = media_analyst(file_path)
	print(result)