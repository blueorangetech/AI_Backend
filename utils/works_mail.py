from auth.works_token_manager import WorksTokenManager
from auth.google_auth_manager import get_bigquery_client
from services.bigquery_service import BigQueryReportService
from utils.http_client_manager import get_http_client
from services.works_service import WorksService
import pandas as pd
import logging, io, os, base64

logger = logging.getLogger(__name__)

async def send_mail(bo_customers: dict):
    # 1. 빅쿼리 데이터 조회
    bigquery_client = get_bigquery_client()
    bigquery_service = BigQueryReportService(bigquery_client)

    result = await bigquery_service.get_data_by_date(
        dataset_id="speed_mate", table_id="NAVER_AD", 
        start_date ="2025-10-22", end_date = "2025-10-22")
    
    # 2. 파일로 변환 (xlsx)
    df = pd.DataFrame(result)
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='openpyxl')
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
                "filename": "report.xlsx",
                "data": base64_content
            }
        ]
    }

    # 3. 메일 전송
    vaild_token = await WorksTokenManager().get_vaild_token()
    works_service = WorksService(vaild_token)
    response = await works_service.send_mail(mail_content)
    return response

async def check_holidays():
    url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo'
    params = {
        "serviceKey": os.environ["OPEN_DATA"],
        "solYear": "2025",
        "solMonth": "10",
    }
    client = await get_http_client()
    response = await client.get(url, params=params)
    logger.info(response)
    result = response.content
    return result
    