from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from auth.works_token_manager import WorksTokenManager
from services.works_service import WorksService
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
from utils import works_mail
from configs.customers_event import bo_customers
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/worksmail", tags=["reports"])
    
@router.get("/test")
async def send_mail():
    ## utils -> Service 이동
    # 여기서 공휴일 정보 먼저 판단하고
    response = await works_mail.check_holidays()
    return response

    # 휴일 다음날이면 휴일 데이터까지 가져와서 메일 보내기
    response = await works_mail.send_mail(bo_customers)

@router.post("/read")
async def read_mails(request: MediaRequestModel):
    try:
        customer = request.customer
        data_set_name = bo_customers[customer]["data_set_name"]
        folder = bo_customers[customer]["media_list"]["criteo"]["mailfolder"]
        report_name = bo_customers[customer]["media_list"]["criteo"]["report_name"]
        field_names = bo_customers[customer]["media_list"]["criteo"]["field_names"]

        token_manager = WorksTokenManager()
        access_token = await token_manager.get_vaild_token()
        service = WorksService(access_token)

        # 메일함에서 읽어오기
        response = await service.read_mails(folder, report_name, field_names)

        data = {"CRITEO": response["data"]}
        delete_mail_ids = response["mail_ids"]

        # BigQuery 연결
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)

        result = await bigquery_service.insert_daynamic_schema(data_set_name, data)

        await service.delete_mails(delete_mail_ids)
        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}