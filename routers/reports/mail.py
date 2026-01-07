from fastapi import APIRouter
from models.media_request_models import MediaRequestModel
from auth.works_token_manager import WorksTokenManager
from services.works_service import WorksService
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
from services import send_report
from datetime import datetime, timedelta
from configs.customers_event import bo_customers
from configs.customer_manager import customer_manager
import pandas as pd
import io, base64
import logging


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/worksmail", tags=["reports"])

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

@router.post("/send")
async def send_mail(request: MediaRequestModel):
    try:
        customer = request.customer
        manager_info = customer_manager[customer]

        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryReportService(bigquery_client)
        result = {}

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        report_info = manager_info["table"]

        # 1. 빅쿼리 데이터 조회
        if manager_info["target_date"] is None:
            for sheet_name in report_info:
                table_name = report_info[sheet_name]
                result[sheet_name] = await bigquery_service.get_all_data(
                    dataset_id=customer, table_id=table_name
                )
        else:
            for sheet_name in report_info:
                table_name = report_info[sheet_name]
                result[sheet_name] = await bigquery_service.get_data_by_date(
                    dataset_id=customer, table_id=table_name,
                    start_date=yesterday_str, end_date=yesterday_str
                )
        
        # 2. 파일로 변환 (xlsx)
        excel_buffer = io.BytesIO()

        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            for sheet_name, data in result.items():
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        excel_content = excel_buffer.getvalue()
        base64_content = base64.b64encode(excel_content).decode('utf-8')

         # 3. 메일 콘텐츠 작성
        mail_content = {
            "to": "chunws@blorange.co.kr",
            "subject": "메일 전송 테스트",
            "body": "테스트 메일 입니다.",
            "userName": "Reporting_System",
            "attachments": [
                {
                    "filename": f"report_{yesterday_str}.xlsx",
                    "data": base64_content
                }
            ]
        }

        # 3. 메일 전송
        vaild_token = await WorksTokenManager().get_vaild_token()
        works_service = WorksService(vaild_token)
        response = await works_service.send_mail(mail_content)
        return response

    except Exception as e:
        return {"status": "error", "message": str(e)}