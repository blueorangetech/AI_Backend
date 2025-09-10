from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from services.naver_service import NaverReportService
from services.kakao_service import KakaoReportService
from services.google_service import GoogleAdsReportServices
from services.ga4_service import GA4ReportServices
from services.meta_service import MetaAdsReportServices
from services.bigquery_service import BigQueryReportService
from auth.kakao_token_manager import KakaoTokenManager
from models.media_request_models import (
    TotalRequestModel,
    MediaRequestModel,
    MetaAdsRequestModel,
)
from auth.naver_auth_manager import get_naver_client, get_gfa_client
from auth.google_auth_manager import (
    get_google_ads_client,
    get_ga4_client,
    get_bigquery_client,
)
from auth.meta_auth_manager import get_meta_ads_client
from configs.customers_event import bo_customers
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/all")
async def create_all_report(request: TotalRequestModel):
    try:
        media_config = {
            "naver": {
                "model_class": MediaRequestModel,
                "handler": create_naver_reports,
            },
            "kakao": {
                "model_class": MediaRequestModel,
                "handler": create_kakao_reports,
            },
            "kakao_moment": {
                "model_class": MediaRequestModel,
                "handler": create_kakao_monent_reports,
            },
            "google_ads": {
                "model_class": MediaRequestModel,
                "handler": create_google_report,
            },
            "ga4": {"model_class": MediaRequestModel, "handler": create_ga4_report},
        }
        result = {}

        for customer in request.customers:
            result[customer] = {}

            for media in bo_customers[customer]["media_list"].keys():    
                if media in media_config:
                    platform_data = {"customer": customer}
                    # 매체별 처리
                    config = media_config[media]
                    request_model = config["model_class"](**platform_data)
                    response = await config["handler"](request_model)

                else:
                    response = "지원하지 않는 매체입니다."

                result[customer][media] = response

        return {"status": "success", "message": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/naver")
async def create_naver_reports(request: MediaRequestModel):
    """네이버 광고 성과 다운로드"""
    try:
        customer = request.customer
        customer_info = bo_customers[customer]["media_list"]["naver"] 
        data_set_name = bo_customers[customer]["data_set_name"]

        customer_id = customer_info["customer_id"]

        master_list = customer_info["master_list"]
        stat_types = customer_info["stat_types"]

        client = get_naver_client(customer_id)
        service = NaverReportService(client)

        response = await service.create_complete_report(master_list, stat_types)
        
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/gfa")
async def create_gfa_reports():
    """GFA 광고 성과 다운로드"""
    client = get_gfa_client()
    response = await client.get_manage_accounts()
    return response

@router.post("/kakao")
async def create_kakao_reports(request: MediaRequestModel):
    """카카오 광고 성과 다운로드"""
    try:
        customer = request.customer
        account_id = bo_customers[customer]["media_list"]["kakao"]["account_id"]
        data_set_name = bo_customers[customer]["data_set_name"]

        token_manager = KakaoTokenManager()
        vaild_token = await token_manager.get_vaild_token()

        service = KakaoReportService(vaild_token, account_id)
        response = await service.create_report()

        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        # 데이터셋, 테이블 생성 후 삽입 (없으면 자동 생성)
        result = bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/kakaomoment")
async def create_kakao_monent_reports(request: MediaRequestModel):
    """카카오 모먼트 광고 성과 다운로드"""
    try:
        customer = request.customer
        account_id = bo_customers[customer]["media_list"]["kakao"]["account_id"]
        data_set_name = bo_customers[customer]["data_set_name"]

        token_manager = KakaoTokenManager()
        valid_token = await token_manager.get_vaild_token()

        service = KakaoReportService(valid_token, account_id)
        response = await service.create_moment_report()

        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/kakao/token")
async def enroll_kakao_token(code: str = Query(...)):
    """Kakao Token이 모두 만료 되면 수동으로 업데이트 진행"""
    try:
        token_manager = KakaoTokenManager()
        await token_manager.renewal_all_token(code)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"message": "카카오 토큰이 성공적으로 등록되었습니다"},
        )

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/google")
async def create_google_report(request: MediaRequestModel):
    """구글 광고 성과 다운로드"""
    try:
        customer = request.customer

        customer_info = bo_customers[customer]["media_list"]["google_ads"]
        data_set_name = bo_customers[customer]["data_set_name"]

        customer_id = customer_info["customer_id"]
        logger.info(f"{customer} google ads id : {customer_id}")

        fields = customer_info["fields"]
        view_level = customer_info["view_level"]
        logger.info(f"{customer} google ads fields : {fields}")

        client = get_google_ads_client(customer_id)
        service = GoogleAdsReportServices(client)

        response = service.create_reports(fields, view_level)

        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        # BigQuery로 보내기
        result = bigquery_service.insert_daynamic_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/ga4")
async def create_ga4_report(request: MediaRequestModel):
    try:
        customer = request.customer
        customer_info = bo_customers[customer]["media_list"]["ga4"]
        data_set_name = bo_customers[customer]["data_set_name"]

        property_id = customer_info["property_id"]

        navigation_reports = customer_info["fields"]

        client = get_ga4_client(property_id)
        service = GA4ReportServices(client)
        
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        # BigQuery로 보내기
        results = {}
        for report_type, data in navigation_reports.items():
            response = service.create_report(data, report_type)
            result = bigquery_service.insert_daynamic_schema(data_set_name, response)
            results.update(result)

        return results

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
        "inline_link_clicks",
    ]
    response = await service.create_reports(fields)
    return response
