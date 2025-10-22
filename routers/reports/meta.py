from fastapi import APIRouter
from services.meta_service import MetaAdsReportServices
from auth.meta_auth_manager import get_meta_ads_client
from models.media_request_models import MediaRequestModel
from auth.google_auth_manager import get_bigquery_client
from services.bigquery_service import BigQueryReportService
from configs.customers_event import bo_customers
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/meta", tags=["reports"])

@router.post("/test")
async def create_meta_reports(request: MediaRequestModel):
    try:
        customer = request.customer
        customer_info = bo_customers[customer]["media_list"]["meta"]
        data_set_name = bo_customers[customer]["data_set_name"]

        account_id = customer_info["account_id"]
        
        client = get_meta_ads_client(account_id)
        service = MetaAdsReportServices(client)

        fields = customer_info["fields"]
        response = await service.create_reports(fields)
        logger.info(response)
        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = await bigquery_service.insert_daynamic_schema(data_set_name, response)

        return result
        return response
    
    except Exception as e:
        return {"status": "error", "message": str(e)}