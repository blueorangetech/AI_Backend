import pandas as pd
import json

def group_data(data: dict, fields: list, sum_fields: list):
        """
        입력한 데이터를 fields 를 기준으로 데이터를 계산한다.
        data는 fields의 길이가 모두 같아한다.
        
        Args:
            data(dict): 연산하고자 하는 데이터의 딕셔너리
            fields: 데이터 유형이 문자형인 필드
            sum_fields : 데이터 유형이 숫자형인 필드

        Returns:
            fields를 기준으로 수치형 데이터 필드의 합산 갑
        """
        print(fields, sum_fields)
        try:
            keys = data.keys()
            arr = [len(data[key]) for key in keys]

            if len(set(arr)) != 1:
                raise ValueError("입력한 data의 모든 key의 길이는 같아야 합니다.")
            
            pd_data = pd.DataFrame(data)
            data_filter = pd_data[fields + sum_fields]
            result = data_filter.groupby(fields).sum().reset_index()
            result = result.to_dict(orient='records')
            return json.dumps(result)
        
        except Exception as e:
            return f"에러 발생: {e}"
        
def group_data_description():
     description = {
            "name": "group_data",
            "description": "입력한 데이터를 fields를 기준으로 계산합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "object",
                        "description": "연산하고자 하는 데이터의 딕셔너리"
                    },
                    "fields": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "데이터 유형이 문자형인 필드"
                    },
                    "sum_fields": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        },
                        "description": "데이터 유형이 숫자형인 필드"
                    }
                },
                "required": ["data", "fields", "sum_fields"]
            }
        }
     
     return description