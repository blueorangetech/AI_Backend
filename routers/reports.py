from fastapi import APIRouter, HTTPException, Path, status, Query
from fastapi.responses import JSONResponse
from database.mongodb import MongoDB
from reports import kakao
from services.naver_service import NaverReportService
from services.kakao_service import KakaoReportService
from auth.kakao_token_manager import KakaoTokenManager
from dependency import get_naver_client
from models.naver_request_models import NaverRequsetModel
from models.kakao_request_models import KakakoRequestModel
import pandas as pd
import time, json, os, jwt, requests

router = APIRouter(prefix="/reports", tags=["reports"])

mongodb = MongoDB.get_instance()
db = mongodb["Customers"]
jwt_token = os.environ["jwt_token_key"]

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

@router.get("/kakao/token")
async def enroll_kakao_token(code: str = Query(...)):
    """ Token이 모두 만료 되면 수동으로 업데이트 진행"""
    try:
        token_manager = KakaoTokenManager()
        await token_manager.renewal_all_token(code)

        return JSONResponse(
            status_code = status.HTTP_201_CREATED,
            content={"message": "카카오 토큰이 성공적으로 등록되었습니다"}
        )
        
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@router.get("/kakao/test")
async def test(request: KakakoRequestModel):
    try:
        token_manager = KakaoTokenManager()
        vaild_token = await token_manager.get_vaild_token()

        service = KakaoReportService(vaild_token, request.account_id)
        result = await service.create_report(request.start_date, request.end_date)

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@router.post("/kakao")
def create_kakao_report():
    token_info = db.get_collection("token").find_one({"media": "kakao"})

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

        db.get_collection("token").update_one({"media": "kakao"}, 
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
    