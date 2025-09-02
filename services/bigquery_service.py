from google.cloud import bigquery
from models.bigquery_schemas import (
    naver_search_ad_schema,
    naver_search_ad_cov_schema,
    naver_shopping_ad_schema,
    naver_shopping_ad_cov_schema,
    kakao_search_ad_schema,
    kakao_moment_ad_schema,
    google_ads_schema,
    ga4_schema,
)
import logging

logger = logging.getLogger(__name__)


class BigQueryReportService:
    """BigQuery 리포트 삽입 관련 비즈니스 로직을 처리하는 서비스"""

    def __init__(self, bigquery_client):
        self.client = bigquery_client
        self.config = {
            "NAVER_AD": naver_search_ad_schema(),
            "NAVER_AD_CONVERSION": naver_search_ad_cov_schema(),
            "NAVER_SHOPPINGKEYWORD_DETAIL" :naver_shopping_ad_schema(),
            "NAVER_SHOPPINGKEYWORD_CONVERSION_DETAI": naver_shopping_ad_cov_schema(),
            "kakao_search_ad": kakao_search_ad_schema(),
            "kakao_moment_ad": kakao_moment_ad_schema(),
            "GOOGLE_ADS": google_ads_schema(),
            "GA4": ga4_schema(),
        }

    def insert_static_schema(self, data_set_name: str, reports_data: dict) -> dict:
        """정적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}

        self.client.create_dataset(data_set_name)

        for table_name, data in reports_data.items():
            result[table_name] = False

            if data:  # 데이터가 있으면
                try:
                    schema = self.config.get(table_name, [])
                    if len(schema) == 0:
                        raise Exception(f"정의된 BigQuery 스키마가 없습니다")

                    self.client.insert_start(data_set_name, table_name, schema, data)
                    result[table_name] = True
                    logger.info(f"{table_name} 데이터 BigQuery 삽입 완료")

                except Exception as e:
                    logger.error(f"{table_name} BigQuery 삽입 실패: {str(e)}")
                    result[table_name] = False
            else:
                logger.warning(f"{table_name} 데이터가 비어있음")

        return result

    def insert_daynamic_schema(self, data_set_name: str, reports_data: dict) -> dict:
        """동적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}

        self.client.create_dataset(data_set_name)

        for table_name, data in reports_data.items():

            schema_name = "GA4" if table_name.startswith("GA4") else table_name
            basic_schema = self.config[schema_name]

            schema = self._create_schema(basic_schema, data)
            result[table_name] = False

            if data:  # 데이터가 있으면
                try:
                    if len(schema) == 0:
                        raise Exception(f"생성된 BigQuery 스키마가 없습니다")

                    self.client.insert_start(data_set_name, table_name, schema, data)
                    result[table_name] = True
                    logger.info(f"{table_name} 데이터 BigQuery 삽입 완료")

                except Exception as e:
                    logger.error(f"{table_name} BigQuery 삽입 실패: {str(e)}")
                    result[table_name] = False
            else:
                logger.warning(f"{table_name} 데이터가 비어있음")

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
                # 데이터 타입을 자동으로 추론
                sample_value = data[0][key]
                if isinstance(sample_value, int):
                    data_type = "INTEGER"
                elif isinstance(sample_value, float):
                    data_type = "FLOAT"
                elif isinstance(sample_value, bool):
                    data_type = "BOOLEAN"
                else:
                    data_type = "STRING"

                schema_fields.append(bigquery.SchemaField(key, data_type))

        return schema_fields
