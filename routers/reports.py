from fastapi import APIRouter, HTTPException, Path
from database.mongodb import MongoDB
from reports import kakao
from services.naver_service import NaverReportService
from dependency import get_naver_client
from models.naver_request_models import NaverRequsetModel
import pandas as pd
import time, json, os

router = APIRouter(prefix="/reports", tags=["reports"])

mongodb = MongoDB.get_instance()
db = mongodb["Customers"]
collection = db["token"]

@router.post("/naver")
async def create_naver_reports(requset: NaverRequsetModel):
    """ 네이버 광고 성과 다운로드 """
    try:
        client = get_naver_client(requset.customer_id)
        service = NaverReportService(client)

        await service.create_complete_report(requset.target_date)
        return 

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "customer_id": requset.customer_id,
            "target_date": requset.target_date
        }
        
@router.post("/kakao")
def create_kakao_report():
    token_info = collection.find_one({"media": "kakao"})

    if token_info is None:
        raise HTTPException(status_code=404, detail="카카오 토큰 정보를 찾을 수 없습니다")
    
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

    if campaigns is None:
        raise HTTPException(status_code=404, detail="해당 광고주를 찾을 수 없습니다.")
    
    groups = {}

    reports = pd.DataFrame(columns=["date", "campaign", "group", "keyword", "imp", "click", "cost"])

    for campaign in campaigns:
        report_part = kakao.get_reports(access_token, "627936", campaign, "20241001", "20241002")
        reports = pd.concat([reports, report_part], ignore_index=True)
        
    for campaign in campaigns:
        group_part = kakao.get_groups(access_token, "627936", campaign)

        if isinstance(group_part, dict):
            groups = {**groups, **group_part}
        
        else:
            raise HTTPException(status_code=404, detail="그룹 호출 오류")

    keywords = {}
    for group in groups:
        keyword_part = kakao.get_keywords(access_token, "627936", group)

        if isinstance(keyword_part, dict):
            keywords = {**keywords, **keyword_part}
        
        else:
            raise HTTPException(status_code=404, detail="키워드 호출 오류")

    reports["campagin_name"] = reports["campaign"].apply(lambda x: campaigns[x])
    reports["group_name"] = reports["group"].apply(lambda x: groups[x])
    reports["keyword_name"] = reports["keyword"].apply(lambda x: keywords[x])
    
    return reports
    