from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from services.naver_service import NaverReportService
from services.kakao_service import KakaoReportService
from services.google_service import GoogleAdsReportServices
from services.ga4_service import GA4ReportServices
from services.meta_service import MetaAdsReportServices
from services.bigquery_service import BigQueryReportService
from auth.kakao_token_manager import KakaoTokenManager
from models.media_request_models import (TotalRequestModel, NaverRequestModel, 
                                         KakaoRequestModel, KakaoMomentRequestModel,
                                         GoogleAdsRequestModel, GA4RequestModel,
                                         MetaAdsRequestModel)
from models.bigquery_schemas import naver_search_ad_schema, kakao_search_ad_schema, kakao_moment_ad_schema
from auth.naver_auth_manager import get_naver_client
from auth.google_auth_manager import get_google_ads_client, get_ga4_client, get_bigquery_client
from auth.meta_auth_manager import get_meta_ads_client
import logging, time, os

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["reports"])

@router.post("/all")
async def create_all_report(request: TotalRequestModel):
    try:
        media_config = {
            "naver": {
                "model_class": NaverRequestModel,
                "handler": create_naver_reports
            },
            "kakao": {
                "model_class": KakaoRequestModel, 
                "handler": create_kakao_reports
            },
            "kakao_moment": {
                "model_class": KakaoMomentRequestModel,
                "handler": create_kakao_monent_reports
            },
            "google_ads": {
                "model_class": GoogleAdsRequestModel,
                "handler": create_google_report
            },
            "ga4": {
                "model_class": GA4RequestModel,
                "handler": create_ga4_report
            }
        }
        result = {}
        for customer in request.customers:
            table_name = customer["table_name"]

            for media in customer["media_list"]:

                if media in media_config:
                    platform_data = {
                        **customer["media_list"][media],
                        "table_name": table_name
                    }
                    # 매체별 처리
                    config = media_config[media]
                    request_model = config["model_class"](**platform_data)
                    response = await config["handler"](request_model)
                    
                else:
                    response = "지원하지 않는 매체입니다."
            
                result[media] = response

        for res in result:
            if not result[res]:
                return {"status": "some success", "message": result}
            
        return {"status": "success", "message": result}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/naver")
async def create_naver_reports(request: NaverRequestModel):
    """ 네이버 광고 성과 다운로드 """
    try:
        customer_id = request.customer_id
        table_name = request.table_name
        stat_types = request.stat_types

        client = get_naver_client(customer_id)
        service = NaverReportService(client)

        response = await service.create_complete_report(stat_types)
        
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)
        
        result = bigquery_service.insert_static_schema(table_name, response)
                
        return result

    except Exception as e:
        return { "status": "error", "message": str(e)}
    
@router.post("/kakao")
async def create_kakao_reports(request: KakaoRequestModel):
    """ 카카오 광고 성과 다운로드 """
    try:
        account_id, table_name = request.account_id, request.table_name
        token_manager = KakaoTokenManager()
        vaild_token = await token_manager.get_vaild_token()

        service = KakaoReportService(vaild_token, account_id)
        response = await service.create_report()
        
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)
        
        # 데이터셋, 테이블 생성 후 삽입 (없으면 자동 생성)
        result = bigquery_service.insert_static_schema(table_name, response)

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@router.post("/kakaomoment")
async def create_kakao_monent_reports(request: KakaoMomentRequestModel):
    """ 카카오 모먼트 광고 성과 다운로드 """
    try:
        account_id, table_name = request.account_id, request.table_name

        token_manager = KakaoTokenManager()
        valid_token = await token_manager.get_vaild_token()

        service = KakaoReportService(valid_token, account_id)
        response = await service.create_moment_report()
        
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = bigquery_service.insert_static_schema(table_name, response)

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}
        

@router.get("/kakao/token")
async def enroll_kakao_token(code: str = Query(...)):
    """ Kakao Token이 모두 만료 되면 수동으로 업데이트 진행 """
    try:
        token_manager = KakaoTokenManager()
        await token_manager.renewal_all_token(code)

        return JSONResponse(
            status_code = status.HTTP_201_CREATED,
            content={"message": "카카오 토큰이 성공적으로 등록되었습니다"}
        )
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/google/test")
async def create_google_report(request: GoogleAdsRequestModel):
    """ 구글 광고 성과 다운로드 """
    try:
        customer_id = request.customer_id
        fields = request.fields
        table_name =  request.table_name

        client = get_google_ads_client(customer_id)
        service = GoogleAdsReportServices(client)

        response = service.create_reports(fields)

        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        # BigQuery로 보내기
        result = bigquery_service.insert_daynamic_schema(table_name, "google_ads", response)

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/ga4/test")
async def create_ga4_report(request: GA4RequestModel):
    try:
        client = get_ga4_client(request.property_id)
        service = GA4ReportServices(client)

        response = service.properties_list()

        return response
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/test")
async def test(request: MetaAdsRequestModel):
    account_id = request.account_id
    table_name = request.table_name

    client = get_meta_ads_client(account_id)
    service = MetaAdsReportServices(client)

    # 임시 변수
    fields = [
                "campaign_name",
                "adset_name",
                "ad_name",
                "impressions",
                "clicks", 
                "spend",
                "inline_link_clicks"
            ]
    response = await service.create_reports(fields)
    return response