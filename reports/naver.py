from .signaturehelper import get_header
from .report_fields import (naver_campaign_fields, naver_ad_group_fields, 
                            naver_keyword_fields, naver_ad_fields, naver_vaild_fields)
import os, requests, csv, io
import pandas as pd

BASE_URL = 'https://api.naver.com'
API_KEY = os.environ["NAVER_API_KEY"]
SECRET_KEY = os.environ["NAVER_SECRET_KEY"]
CUSTOMER_ID = os.environ["NAVER_CUSTOMER_ID"]

def create_headers(method, uri):
    headers = get_header(method=method, uri=uri, api_key=API_KEY, secret_key=SECRET_KEY, customer_id=CUSTOMER_ID)
    return headers

def create_stat_report(type, state_date):
    uri = "/stat-reports"
    headers = create_headers("POST", uri)
    data = {"reportTp": type, "statDt": state_date}
    
    response = requests.post(BASE_URL + uri, headers=headers, json=data)
    if response.status_code == 200:
        info = response.json()["reportJobId"]
        return info

def create_master_report(type):
    uri = "/master-reports"
    headers = create_headers("POST", uri)
    data = {"item": type}
    
    response = requests.post(BASE_URL + uri, headers=headers, json=data)

    if response.status_code == 201:
        info = response.json()["id"]
        return info

def check_status(uri, report_id):
    uri = f"{uri}/{report_id}"
    headers = create_headers("GET", uri)

    response = requests.get(BASE_URL + uri, headers=headers)
    if response.status_code == 200:
        res = response.json()

        if res["status"] == "BUILT":
            download_url = res["downloadUrl"]
            return download_url

def download_report(download_url, file_name):
    uri = "/report-download"
    headers = create_headers("GET", uri)

    file_name = f"file_repository/{file_name}_report.csv"

    response = requests.get(download_url, headers=headers)
    if response.status_code == 200:
        # 파일 저장
        tsv_data = response.content.decode("utf-8")
        tsv_reader = csv.reader(io.StringIO(tsv_data), delimiter="\t")

        with open(file_name, 'w', encoding="utf-8", newline="") as file:
            file_writer = csv.writer(file, delimiter=",")
            for row in tsv_reader:
                file_writer.writerow(row)
    
        return file_name

def delete_report(uri, report_id):
    uri = f"{uri}/{report_id}"
    headers = create_headers("DELETE", uri)
    requests.delete(BASE_URL + uri, headers=headers)

def merge_reports(report_list):
    header_list = [naver_campaign_fields(), naver_ad_group_fields()
                   ,naver_keyword_fields(), naver_ad_fields()]
    
    data = {"campaign": "", "group": "", "keyword": "", "ad_result": ""}

    for key, report, header in zip(data, report_list, header_list):
        data[key] = pd.read_csv(report, header=None, names=header)
    
    master_header = pd.merge(data["campaign"], data["group"], on="CampaignID", how="left")
    master_header = pd.merge(master_header, data["keyword"], on="AdGroupID", how="left")
    
    report = pd.merge(data["ad_result"], master_header, on=["CampaignID", "AdGroupID", "AdKeywordID"], how="left")
    # print(master_header.columns)
    vaild_header = naver_vaild_fields()

    result = report[vaild_header].fillna(0)
    
    result.to_csv("result.csv",index=False, encoding="euc-kr")

    return result

if __name__ == "__main__":
    result = merge_reports(["Campaign_report.csv", "Adgroup_report.csv", "Keyword_report.csv", "AD_report.csv"])
    print(result)
