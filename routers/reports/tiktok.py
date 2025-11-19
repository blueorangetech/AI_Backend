from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from configs.customers_event import bo_customers
from clients.tiktok_api_client import TikTokAPIClient
from services.tiktok_service import TikTokReportService
from auth.google_auth_manager import get_bigquery_client
from services.bigquery_service import BigQueryReportService
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tiktok", tags=["reports"])

@router.post("/")
async def create_tiktok_reports(request: MediaRequestModel):
    try:
        customer = request.customer
        customer_info = bo_customers[customer]["media_list"]["tiktok"]
        data_set_name = bo_customers[customer]["data_set_name"]

        account_id = customer_info["account_id"]
        dimensions = customer_info["dimensions"]
        metrics = customer_info["metrics"]

        client = TikTokAPIClient(account_id)
        service = TikTokReportService(client)

        response = await service.create_report(dimensions, metrics)
        
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = await bigquery_service.insert_daynamic_schema(data_set_name, response)

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}