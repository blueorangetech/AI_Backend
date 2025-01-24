from gpt_models.gpt_option import interim_report
from gpt_models.keyword_analyst import makerter_description
from gpt_models.general_analyst import general_analyst_description

from tools.group_data import group_data
from tools.keyword_similartiy import check_similarity

import pandas as pd
import time, traceback


## CHAT GPT ###
def analyst(pre_result, standard, compare, fields):
	try:
		print("Processing Total Analyst")
		print(f"Standard: {[dt.strftime('%Y-%m-%d %Z') for dt in standard]}")
		print(f"Compare: {[dt.strftime('%Y-%m-%d %Z') for dt in compare]}")

		arguments = {"data": pre_result, "standard": ["기준"],
					"fields": [], "sum_fields" : fields, "limit": None}

		total_data = group_data(arguments)
		
		agent = general_analyst_description()
		
		req = f"""제공되는 모든 데이터를 분석 한 후 보고서를 작성하라.
				데이터는 생략할 수 없다.
				수 표기 시 회계처리를 해줘
				분석 기간 : {standard}
				비교 기간 : {compare}
				{total_data}"""
		msg = {"role": "user", "content": req}

		print("Wait for GPT response...")
		response = interim_report(msg, agent)
		result = response.replace("```html", "").replace("```", "")	
		
		return result
	
	except:
		raise ValueError('데이터 처리 중 문제가 발생했습니다.')

def media_analyst(pre_result, standard, compare, fields, depth, product):
	try:
		print(f"Processing Media Analyst")
		print(f"Standard: {[dt.strftime('%Y-%m-%d %Z') for dt in standard]}")
		print(f"Compare: {[dt.strftime('%Y-%m-%d %Z') for dt in compare]}")
		
		agent = general_analyst_description()
		
		total_data = []
		if len(product): # 상품이 여러 개 존재하는 광고주
			product_list = pre_result[product[0]].unique()

			for pl in product_list:
				pre_result_part = pre_result.loc[pre_result[product[0]] == pl]
				product_result = []

				for field in fields:
					arguments = {"data": pre_result_part, "standard": ["기준"],
							"fields": depth, "sum_fields" : field, "limit": 1}
					
					part_data = group_data(arguments)

					# 빈 데이터는 요청에 추가하지 않음
					if len(part_data["data"]): 
						part_result = {"product": pl, "part_data": part_data}
						product_result.append(part_result)
				
				req = f"""다음 데이터를 상품과 지표 별로 
				테이블을 분리하여 보고서를 작성하라.
				수 표기 시 회계처리를 해줘
				분석 기간 : {standard}
				비교 기간 : {compare}
				{product_result}"""

				msg = {"role": "user", "content": req}

				product_response = interim_report(msg, agent)
				total_data.append(product_response.replace("```html", "").replace("```", ""))
			
			return "\n".join(total_data)
		
		else:
			for field in fields:
				arguments = {"data": pre_result, "standard": ["기준"],
						"fields": depth, "sum_fields" : field, "limit": 1}
				
				total_data.append(group_data(arguments))
				
		msg = {"role": "user", "content": req}
		req = f"""다음 데이터를 상품과 지표 별로 
				테이블을 분리하여 보고서를 작성하라.
				수 표기 시 회계처리를 해줘
				분석 기간 : {standard}
				비교 기간 : {compare}
				{total_data}"""
		
		print("Wait for GPT response...")
		response = interim_report(msg, agent)
		result = response.replace("```html", "").replace("```", "")
		
		return result
	
	except:
		traceback.print_exc()
		raise ValueError('데이터 처리 중 문제가 발생했습니다.')

def keyword_analyst(pre_result, standard, compare, fields, depth):
	try:
		print("Processing Keyword Analyst")

		# 키워드 유사도 기준 그룹화
		keywords = pre_result["키워드"].str.lower().drop_duplicates(keep='first')
		unique_keywords = keywords.to_list()

		print("Grouping Start")
		groups_map, groups_index = check_similarity(unique_keywords) # 90 seconds
		
		pre_result["키워드그룹"] = pre_result["키워드"].map(groups_map)
		
		## 매체 분석
		response = {}
		agent = makerter_description()

		for field in fields:
			start = time.time()
			# 데이터 1차 분석 - 등락 폭이 가장 심한 키워드 그룹

			arguments = {"data": pre_result, "standard": ["기준"],
					"fields": ["키워드그룹"], "sum_fields" : field, "limit": 1}
			
			total_data = group_data(arguments)
			
			if total_data.empty:
				continue

			data_chunks = []

			# 데이터 2차 분석 - 키워드 그룹 기준, 
			for keyword in total_data["키워드그룹"]:
				keyword_data = pre_result[pre_result["키워드그룹"] == keyword]
				arguments = {"data": keyword_data, "standard": ["기준"],
					"fields": depth + ["키워드그룹"], "sum_fields" : field, "limit": 1}
				
				chunk = group_data(arguments)
				chunk["키워드그룹"] = chunk["키워드그룹"].map(groups_index)
				data_chunks.append(chunk)

			total_data["키워드그룹"] = total_data["키워드그룹"].map(groups_index)
			media_data = pd.concat(data_chunks)
			req = f"""
					다음 데이터를 분석하시오
					수 표기 시 회계처리를 해줘
					분석 기간 : {standard}
					비교 기간 : {compare}
					total data: {total_data}
					media data: {media_data}
					"""
					
			msg = {"role": "user", "content": req}

			result = interim_report(msg, agent)
			
			if isinstance(field, str):
				response[field] = result.replace("```html", "").replace("```", "")	
			
			else:
				response[field[0]] = result.replace("```html", "").replace("```", "")	

			end = time.time()
			print(f'{field} Processing time: {end - start}')

		return response
	
	except:
		print(traceback.format_exc())
		raise ValueError('데이터 처리 중 문제가 발생했습니다.')
	
if __name__ == "__main__":
	file_path = "C:/Users/blueorange/Desktop/캐롯_리포트.xlsx"
	result = media_analyst(file_path)
	print(result)