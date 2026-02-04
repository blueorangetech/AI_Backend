from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
import logging
from typing import Optional, List

router = APIRouter(prefix="/search", tags=["search"])
logger = logging.getLogger(__name__)

def get_bq_service():
    """BigQuery 리포트 서비스 초기화"""
    bigquery_client = get_bigquery_client()
    return BigQueryReportService(bigquery_client)

@router.get("/bigquery/all")
async def get_all_data(dataset_id: str, table_id: str):
    """특정 테이블의 전체 데이터를 조회"""
    try:
        service = get_bq_service()
        result = await service.get_all_data(dataset_id, table_id)
        return {'status': 'success', 'data': result}
    
    except Exception as e:
        logger.error(f"전체 데이터 조회 실패: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/bigquery/date")
async def get_data_by_date(
    dataset_id: str, 
    table_id: str, 
    start_date: str, 
    end_date: str,
    limit: Optional[int] = None,
    offset: int = 0
):
    """특정 날짜 범위의 데이터를 조회"""
    try:
        service = get_bq_service()
        result = await service.get_data_by_date(dataset_id, table_id, start_date, end_date, limit, offset)
        return {'status': 'success', 'data': result}
    
    except Exception as e:
        logger.error(f"날짜별 데이터 조회 실패: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
