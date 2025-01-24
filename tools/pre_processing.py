from datetime import datetime
import pandas, json, pytz

kst = pytz.timezone('Asia/Seoul')

def date_group(file, standard, compare, input_columns):
    st, com = json.loads(standard), json.loads(compare)

    standard_date = [datetime.fromisoformat(date.replace('Z', '+00:00')).astimezone(kst)
        for date in st]

    compare_date = [datetime.fromisoformat(date.replace('Z', '+00:00')).astimezone(kst)
        for date in com]

    st_start, st_end = standard_date[0].date(), standard_date[1].date()
    com_start, com_end = compare_date[0].date(), compare_date[1].date()

    def classify(date):
        date = date.date()

        if st_start <= date <= st_end:
            return f"분석 기간"
        
        elif com_start <= date <= com_end:
            return f"비교 기간"
        
        else:
            return None

    report_file = pandas.read_excel(file)
    
    columns = report_file.columns

    for column in input_columns:
        if isinstance(column, str): # 단일 필드 (ex. 노출)
            if column not in columns:
                raise ValueError(f"필드명 '{column}': 파일에 존재하지 않습니다.")
            
        else: # 수식 필드 (ex. CTR)
            output, column_1, column_2 = column[0], column[1], column[2]
            for col in [column_1, column_2]:
                if col not in columns:
                    raise ValueError(f"수식 {output}의 '{col}': 파일에 존재하지 않습니다.")


    report_file = report_file.rename(columns={"일별": "날짜"})
    
    report_file["날짜"] = pandas.to_datetime(report_file["날짜"])
    
    report_file["기준"] = report_file["날짜"].apply(classify)
    report_file.dropna(subset=["기준"], inplace=True)
    
    return report_file, standard_date, compare_date