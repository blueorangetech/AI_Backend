from typing import Optional
from google.cloud import bigquery
from . import query_processor
import logging, re

logger = logging.getLogger(__name__)


class BigQueryFetchService:
    """BigQuery 결과값을 리턴하는 비즈니스 로직을 처리하는 서비스"""

    def __init__(self, bigquery_client):
        self.client = bigquery_client
        self.config = {}
    
    async def get_custom_query_result(self, dataset_id: str, table_id: str, report_type: str,
                                      start_date, end_date, limit: Optional[int]  = None, offset: int = 0,
                                      min_cost: Optional[float] = None, min_distribution: Optional[float] = None):
        """커스텀 쿼리 결과 조회"""
        try:
            query = query_processor.BoCustomerQuery.get_query(dataset_id, table_id, report_type,
                                                              start_date, end_date, limit, offset,
                                                              min_cost, min_distribution)
            
            data = await self.client.execute_bigqeury_with_project(query)
            result = []
            
            # 결과를 딕셔너리 리스트로 변환
            for row in data:
                result.append(dict(row))

            logger.info(f"Retrieved {len(result)} rows from {dataset_id}.{table_id}")

            return result
        except Exception as e:
            logger.error(f"데이터 조회 실패: {str(e)}")
            raise e

    async def get_data_by_date(self, dataset_id: str, table_id: str, 
                               start_date: str, end_date: str, limit: Optional[int]  = None, offset: int = 0):
        """특정 테이블의 특정 날짜 데이터를 조회"""
        try:
            data = await self.client.query_data_by_date(dataset_id, table_id, start_date, end_date, limit, offset)
            result = []
            
            # 결과를 딕셔너리 리스트로 변환
            for row in data:
                result.append(dict(row))

            logger.info(f"Retrieved {len(result)} rows from {dataset_id}.{table_id} for date {start_date} - {end_date}")

            return result
        except Exception as e:
            logger.error(f"데이터 조회 실패: {str(e)}")
            raise e

    async def get_dataset_list(self):
        """BigQuery 데이터셋 목록 조회"""
        try:
            datasets = await self.client.list_datasets()
            return datasets
        
        except Exception as e:
            logger.error(f"데이터셋 목록 조회 실패: {str(e)}")
            return f"데이터셋 목록 조회 실패: {str(e)}"
    
    async def get_table_list(self, dataset_id: str):
        """BigQuery 테이블 목록 조회"""
        try:
            tables = await self.client.list_tables(dataset_id)
            return tables
        
        except Exception as e:
            logger.error(f"테이블 목록 조회 실패: {str(e)}")
            return f"테이블 목록 조회 실패: {str(e)}"
    
    async def get_table_schema(self, dataset_id: str, table_id: str):
        """BigQuery 테이블 스키마 조회"""
        try:
            schema = await self.client.get_table_schema(dataset_id, table_id)
            return schema
        
        except Exception as e:
            logger.error(f"테이블 스키마 조회 실패: {str(e)}")
            return f"테이블 스키마 조회 실패: {str(e)}"
    
    async def execute_bigquery_sql(self, sql: str):
        """BigQuery SQL 쿼리 실행"""
        stripped_sql = sql.strip().upper()
        if not stripped_sql.startswith("SELECT"):
            return "SELECT로 시작하는 쿼리문만 허용됨"
    
        prohibited_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", 
        "ALTER", "CREATE", "GRANT", "REVOKE", "MERGE"
        ]

        for keyword in prohibited_keywords:
            if re.search(rf"\b{keyword}\b", stripped_sql):
                return f"금지된 문법 사용 {keyword}"
            
        sql_result = await self.client.execute_bigquery_sql(sql)
        return sql_result