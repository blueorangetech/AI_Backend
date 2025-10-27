from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from services.google_service import GoogleAdsReportServices
from services.ga4_service import GA4ReportServices
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_google_ads_client, get_ga4_client, get_bigquery_client
from configs.customers_event import bo_customers
import logging


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/google", tags=["reports"])

@router.post("/ads")
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
        result = await bigquery_service.insert_daynamic_schema(data_set_name, response)

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
            result = await bigquery_service.insert_daynamic_schema(data_set_name, response)
            results.update(result)

        return results

    except Exception as e:
        return {"status": "error", "message": str(e)}
