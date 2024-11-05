from datetime import datetime, timedelta
import pandas, json

def date_group(file, standard, compare):
    st, com = json.loads(standard), json.loads(compare)

    standard_date = [datetime.strptime(date[:10], "%Y-%m-%d") + timedelta(days=1) for date in st]
    compare_date = [datetime.strptime(date[:10], "%Y-%m-%d") + timedelta(days=1) for date in com]

    st_start, st_end = pandas.Timestamp(standard_date[0]), pandas.Timestamp(standard_date[1])
    com_start, com_end = pandas.Timestamp(compare_date[0]), pandas.Timestamp(compare_date[1])

    def classify(date):
        if st_start <= date <= st_end:
            return f"분석 기간: {st_start.date()} ~ {st_end.date()}"
        
        elif com_start <= date <= com_end:
            return f"비교 기간 : {com_start.date()} ~ {com_end.date()}"
        
        else:
            return None

    report_file = pandas.read_excel(file)
    report_file["날짜"] = pandas.to_datetime(report_file["날짜"])

    report_file["기준"] = report_file["날짜"].apply(classify)
    report_file.dropna(subset=["기준"], inplace=True)

    return report_file