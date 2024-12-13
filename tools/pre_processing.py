from datetime import datetime
import pandas, json, pytz

kst = pytz.timezone('Asia/Seoul')

def date_group(file, standard, compare):
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
            return f"분석 기간: {st_start} ~ {st_end}"
        
        elif com_start <= date <= com_end:
            return f"비교 기간 : {com_start} ~ {com_end}"
        
        else:
            return None

    report_file = pandas.read_excel(file)
    # print(report_file.columns)

    report_file = report_file.rename(columns={"일별": "날짜"})
    
    report_file["날짜"] = pandas.to_datetime(report_file["날짜"])
    
    report_file["기준"] = report_file["날짜"].apply(classify)
    report_file.dropna(subset=["기준"], inplace=True)
    
    return report_file, standard_date, compare_date