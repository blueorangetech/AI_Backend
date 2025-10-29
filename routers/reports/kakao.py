from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from services.kakao_service import KakaoReportService
from auth.kakao_token_manager import KakaoTokenManager
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
from configs.customers_event import bo_customers
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/kakao", tags=["reports"])

@router.post("/keyword")
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
        result = await bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/moment")
async def create_kakao_monent_reports(request: MediaRequestModel):
    """카카오 모먼트 광고 성과 다운로드"""
    try:
        customer = request.customer
        account_id = bo_customers[customer]["media_list"]["kakao_moment"]["account_id"]
        data_set_name = bo_customers[customer]["data_set_name"]

        token_manager = KakaoTokenManager()
        valid_token = await token_manager.get_vaild_token()

        service = KakaoReportService(valid_token, account_id)
        response = await service.create_moment_report()

        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = await bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}
    

@router.post("/test")
async def test(request: MediaRequestModel):
    """카카오 모먼트 광고 성과 다운로드"""
    try:
        customer = request.customer
        account_id = bo_customers[customer]["media_list"]["kakao_moment"]["account_id"]

        token_manager = KakaoTokenManager()
        valid_token = await token_manager.get_vaild_token()

        service = KakaoReportService(valid_token, account_id)
        response = await service.test_api()

        return response

    except Exception as e:
        return {"status": "error", "message": str(e)}
