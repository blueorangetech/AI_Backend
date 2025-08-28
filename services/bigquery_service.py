from google.cloud import bigquery
from models.bigquery_schemas import (naver_search_ad_schema, naver_search_ad_cov_schema, 
                                     kakao_search_ad_schema, kakao_moment_ad_schema, google_ads_schema)
import logging

logger = logging.getLogger(__name__)

class BigQueryReportService:
    """BigQuery 리포트 삽입 관련 비즈니스 로직을 처리하는 서비스"""
    
    def __init__(self, bigquery_client):
        self.client = bigquery_client
        self.config = {
            "naver_search_ad": naver_search_ad_schema(),
            "naver_search_ad_conv": naver_search_ad_cov_schema(),
            "kakao_search_ad": kakao_search_ad_schema(),
            "kakao_moment_ad": kakao_moment_ad_schema(),
            "google_ads": google_ads_schema(),
        }
    
    def insert_static_schema(self, table_name: str, reports_data: dict) -> dict:
        """정적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}
        
        self.client.create_dataset(table_name)
        
        for report_type, data in reports_data.items():
            result[report_type] = False
            
            if data:  # 데이터가 있으면
                try:
                    schema = self.config.get(report_type, [])
                    if len(schema) == 0:
                        raise Exception(f"정의된 BigQuery 스키마가 없습니다")
                    
                    self.client.insert_start(table_name, report_type, schema, data)
                    result[report_type] = True
                    logger.info(f"{report_type} 데이터 BigQuery 삽입 완료")

                except Exception as e:
                    logger.error(f"{report_type} BigQuery 삽입 실패: {str(e)}")
                    result[report_type] = False
            else:
                logger.warning(f"{report_type} 데이터가 비어있음")
        
        return result
    
    def insert_daynamic_schema(self, table_name: str, media: str, reports_data: dict) -> dict:
        """동적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}
        
        self.client.create_dataset(table_name)

        basic_schema = self.config[media]

        for report_type, data in reports_data.items():
            schema = self._create_schema(basic_schema, reports_data[media])
            result[report_type] = False
            
            if data:  # 데이터가 있으면
                try:
                    schema = self.config.get(report_type, [])
                    if len(schema) == 0:
                        raise Exception(f"정의된 BigQuery 스키마가 없습니다")
                    
                    self.client.insert_start(table_name, report_type, schema, data)
                    result[report_type] = True
                    logger.info(f"{report_type} 데이터 BigQuery 삽입 완료")

                except Exception as e:
                    logger.error(f"{report_type} BigQuery 삽입 실패: {str(e)}")
                    result[report_type] = False
            else:
                logger.warning(f"{report_type} 데이터가 비어있음")
        
        return result
        
    
    def _create_schema(self, basic_schema: str, data: list):
        if not data:
            return []
        
        schema_fields = []

        for key in data[0]:
            if key in basic_schema:
                data_type = basic_schema[key]
                schema_fields.append(bigquery.SchemaField(key, data_type))

            else:
                raise Exception(f"Unknown field '{key}' not found in field_index")
            
        return schema_fields