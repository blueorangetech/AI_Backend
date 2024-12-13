import pandas as pd
import numpy as np

def group_data(arguments):
    data, standrad, fields = arguments["data"], arguments["standard"], arguments["fields"]
    sum_fields, limit = arguments["sum_fields"], arguments["limit"]
    
    if limit is None: # 기간 전체 데이터를 비교할 때
        filterd = data.groupby(fields + standrad).sum(numeric_only=True).reset_index()
        carculate_fields = []
        try:
            for field in sum_fields:
                if isinstance(field, list):
                    field_name, a, b, c = field
                    filterd[field_name] = filterd.apply(lambda x: x[a] / x[b] if x[b] > 0 else None, axis=1) * c
                    carculate_fields.append(field_name)
                
                else:
                    carculate_fields.append(field)

            data_sorted = filterd.sort_values(by="기준", key=lambda x: x.str.contains("분석 기간"), ascending=True).reset_index(drop=True)

            for field in carculate_fields:
                data_sorted[f"{field}증감"] = data_sorted[field].diff()
                data_sorted[f"{field}증감%"] = data_sorted[field].pct_change() * 100

            return data_sorted
        
        except Exception as e:
            print(str(e))

    if isinstance(sum_fields, str): # 노출 등 단일 지표를 구할 때
        col_name = sum_fields
        filterd = data.groupby(fields + standrad)[sum_fields].sum(numeric_only=True).reset_index()
    
    else: # CPC 등 비율 지표를 구할 때
        col_name, target1, target2, c = sum_fields
        filterd = data.groupby(fields + standrad)[[target1, target2]].sum(numeric_only=True).reset_index()

        filterd[col_name] = filterd.apply(lambda x: x[target1] / x[target2] if x[target2] > 0 else None, axis= 1) * c
        filterd = filterd.drop(columns = [target1, target2])

    filterd = filterd.sort_values(by="기준", key=lambda x: x.str.contains("분석 기간"), ascending=True).reset_index(drop=True)
    
    filterd["증감 수치"] = filterd.groupby(fields)[col_name].diff()
    filterd["증감률"] = filterd.groupby(fields)[col_name].pct_change() * 100
    
    filterd["증감 수치"], filterd["증감률"] = filterd["증감 수치"].round(2), filterd["증감률"].round(2)

    # 기준 필드 날리고, inf, NaN, -100 삭제하기
    filterd = filterd[(filterd["증감률"].notna()) & (filterd["증감률"] > -100) & (~filterd["증감률"].isin([np.inf, -np.inf]) )]
    sorted_data = filterd.dropna(subset=["증감 수치"]).sort_values(by="증감 수치", ascending=False)
    
    sorted_data = sorted_data[~sorted_data["기준"].str.contains("비교", na=False)] # 비교 기간 삭제
    sorted_data = sorted_data.drop(columns = ["기준"])
    
    head_data = sorted_data[:limit]
    tail_data = sorted_data[-limit:]

    total_data = pd.concat([head_data, tail_data])
    
    return total_data