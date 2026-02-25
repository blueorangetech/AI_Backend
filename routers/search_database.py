from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from services.bigquery_fetch_service import BigQueryFetchService
from auth.google_auth_manager import get_bigquery_client
import logging
from typing import Optional, List

router = APIRouter(prefix="/search", tags=["search"])
logger = logging.getLogger(__name__)

@router.get("/bigquery/date")
async def get_data_by_date(
    dataset_id: str, 
    table_id: str,
    report_type: str,
    start_date: str, 
    end_date: str,
    limit: Optional[int] = None,
    offset: int = 0,
    min_cost: Optional[float] = None,   # 추가: 최소 비용 필터
    min_distribution: Optional[float] = None,    # 추가: 최대 cpa 필터

):
    """특정 날짜 범위의 데이터를 조회"""
    try:
        bigquery_client = get_bigquery_client()
        bigquery_service = BigQueryFetchService(bigquery_client)
        result = await bigquery_service.get_custom_query_result(dataset_id, table_id, report_type,
                                                                start_date, end_date, limit, offset,
                                                                min_cost, min_distribution)
        return {'status': 'success', 'data': result}
    
    except Exception as e:
        logger.error(f"날짜별 데이터 조회 실패: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
