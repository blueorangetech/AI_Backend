import pandas as pd

def group_data(arguments):
    data, standrad, fields = arguments["data"], arguments["standard"], arguments["fields"]
    sum_fields, limit = arguments["sum_fields"], arguments["limit"]
    
    if limit is None: # 기간 전체 데이터를 비교할 때
        filterd = data.groupby(fields + standrad)[sum_fields].sum().reset_index()
        ratio = {"CTR": ["클릭", "노출", 100], "CPC": ["광고비", "클릭", 100], "CPA": ["광고비", "PA청약", 1]}

        for key in ratio:
            a, b, c = ratio[key]
            filterd[key] = filterd.apply(lambda x: x[a] / x[b] if x[b] > 0 else None, axis=1) * c
            sum_fields.append(key)

        for sum_field in sum_fields:
            filterd[f"{sum_field}증감"] = filterd[sum_field].diff()
            filterd[f"{sum_field}증감%"] = filterd[sum_field].pct_change() * 100
        
        return filterd

    if isinstance(sum_fields, str): # 노출 등 단일 지표를 구할 때
        filterd = data.groupby(fields + standrad)[sum_fields].sum().reset_index()

        filterd[f"{sum_fields}증감"] = filterd.groupby(fields)[sum_fields].diff()
        filterd[f"{sum_fields}증감%"] = filterd.groupby(fields)[sum_fields].pct_change() * 100
        sorted_data = filterd.dropna(subset=[f"{sum_fields}증감"]).sort_values(by=f"{sum_fields}증감")
    
    else: # CPC 등 비율 지표를 구할 때
        col_name, target1, target2, c = sum_fields
        filterd = data.groupby(fields + standrad)[[target1, target2]].sum().reset_index()

        filterd[col_name] = filterd.apply(lambda x: x[target1] / x[target2] if x[target2] > 0 else None, axis= 1) * c

        filterd[f"{col_name}증감"] = filterd.groupby(fields)[col_name].diff()
        filterd[f"{col_name}증감%"] = filterd.groupby(fields)[col_name].pct_change() * 100
        
        filterd = filterd.drop(columns = [target1, target2])
        sorted_data = filterd.dropna(subset=[f"{col_name}증감"]).sort_values(by=f"{col_name}증감")

    head_data = sorted_data[:limit]
    tail_data = sorted_data[-limit:]

    total_data = pd.concat([head_data, tail_data])
    
    return total_data