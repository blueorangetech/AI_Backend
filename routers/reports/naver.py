from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from services.naver_service import NaverReportService
from auth.gfa_token_manager import GFATokenManager
from clients.gfa_api_client import GFAAPIClient
from services.gfa_service import GFAReportService
from auth.naver_auth_manager import get_naver_client, get_gfa_client
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
from configs.customers_event import bo_customers
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/naver", tags=["reports"])

@router.post("/ads")
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

        result = await bigquery_service.insert_static_schema(data_set_name, response)

        return result

    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.post("/gfa")
async def create_gfa_reports(request: MediaRequestModel):
    """GFA 광고 성과 다운로드"""
    try:
        token_manager = GFATokenManager()
        access_token = await token_manager.get_vaild_token()

        client = get_gfa_client(access_token, "3419")
        service = GFAReportService(client)
        response = await service.get_performance_data()
        return response

    except Exception as e:
        return {"status": "error", "message": str(e)}