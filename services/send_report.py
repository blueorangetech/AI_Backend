from auth.works_token_manager import WorksTokenManager
from auth.google_auth_manager import get_bigquery_client
from services.bigquery_service import BigQueryReportService
from utils.http_client_manager import get_http_client
from services.works_service import WorksService
from datetime import datetime, timedelta
import pandas as pd
import logging, io, os, base64

logger = logging.getLogger(__name__)

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
    