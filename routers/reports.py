from fastapi import APIRouter
from database.mongodb import MongoDB
from reports import naver, kakao
import pandas as pd
import time, json, os

router = APIRouter(prefix="/reports", tags=["reports"])

mongodb = MongoDB.get_instance()
db = mongodb["Customers"]
collection = db["token"]

@router.post("/naver")
def create_naver_reports():
    master_list = ["Campaign", "Adgroup", "Keyword"]
    stat_list = ["AD"]
    file_list = []

    for master in master_list:
        id = naver.create_master_report(master)

        dounload_url = None
        count = 1
        while dounload_url is None:
            dounload_url = naver.check_status("/master-reports", id)
            count += 1
            time.sleep(0.5)
            if count == 10:
                print("리포트 생성 에러")
                break

        file_name = naver.download_report(dounload_url, master)
        file_list.append(file_name)

        naver.delete_report("/master-reports", id)

    for stat in stat_list:
        id = naver.create_stat_report(stat, "20250121")

        dounload_url = None
        count = 1
        while dounload_url is None:
            dounload_url = naver.check_status("/stat-reports", id)
            count += 1
            time.sleep(0.5)
            if count == 10:
                print("리포트 생성 에러")
                break

        file_name = naver.download_report(dounload_url, stat)
        file_list.append(file_name)
        
        naver.delete_report("/stat-reports", id)
    
    result = naver.merge_reports(file_list)

    for file in file_list:
        if os.path.exists(file):
            os.remove(file)

    return

@router.post("/kakao")
def create_kakao_report():
    token_info = collection.find_one({"media": "kakao"})
    
    client_id = token_info["client_id"]
    access_token = token_info["access_token"]
    refresh_token = token_info["refresh_token"]

    if kakao.check_token(access_token) == False:
        print("access token expired !")
        new_token = kakao.renewal_token(refresh_token, client_id)
        
        access_token = new_token["access_token"]

        if new_token["refresh_token"] is not None:
            print("refresh token renewed !")
            refresh_token = new_token["refresh_token"]

        collection.update_one({"media": "kakao"}, 
                               {"$set": {"access_token": access_token,
                                         "refresh_token": refresh_token}
                                        })
        
    campaigns = kakao.get_campagins(access_token, "627936")
    groups = {}

    reports = pd.DataFrame(columns=["date", "campaign", "group", "keyword", "imp", "click", "cost"])

    for campaign in campaigns:
        report_part = kakao.get_reports(access_token, "627936", campaign, "20241001", "20241002")
        reports = pd.concat([reports, report_part], ignore_index=True)
        
    for campaign in campaigns:
        group_part = kakao.get_groups(access_token, "627936", campaign)
        groups = {**groups, **group_part}

    keywords = {}
    for group in groups:
        keyword_part = kakao.get_keywords(access_token, "627936", group)
        keywords = {**keywords, **keyword_part}
    

    reports["campagin_name"] = reports["campaign"].apply(lambda x: campaigns[x])
    reports["group_name"] = reports["group"].apply(lambda x: groups[x])
    reports["keyword_name"] = reports["keyword"].apply(lambda x: keywords[x])
    
    return reports
    