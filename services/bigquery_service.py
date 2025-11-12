from google.cloud import bigquery
from models.bigquery_schemas import (
    naver_search_ad_schema,
    naver_search_ad_cov_schema,
    naver_shopping_ad_schema,
    naver_shopping_ad_cov_schema,
    naver_gfa_schema,
    kakao_search_ad_schema,
    kakao_moment_ad_schema,
    google_ads_schema,
    ga4_schema,
    meta_schema,
    criteo_schema,
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
            "NAVER_SHOPPINGKEYWORD_CONVERSION_DETAIL": naver_shopping_ad_cov_schema(),
            "NAVER_GFA": naver_gfa_schema(),
            "KAKAO_SEARCH": kakao_search_ad_schema(),
            "KAKAO_MOMENT": kakao_moment_ad_schema(),
            "GOOGLE_ADS": google_ads_schema(),
            "GA4": ga4_schema(),
            "META": meta_schema(),
            "CRITEO": criteo_schema()
        }

    async def insert_static_schema(self, data_set_name: str, reports_data: dict) -> dict:
        """정적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}

        await self.client.create_dataset(data_set_name)

        for table_name, data in reports_data.items():
            result[table_name] = False

            if data:  # 데이터가 있으면
                try:
                    # 날짜 필드명 결정
                    date_field = 'segments_date' if "GOOGLE_ADS" in table_name else 'date'
                    insert_date = data[0][date_field]

                    if await self.client.check_date_exists(data_set_name, table_name, insert_date, date_field):
                        logger.warning(f"{table_name}: 해당 날짜({insert_date})의 데이터가 이미 존재합니다. 삽입을 취소합니다.")
                        result[table_name] = "skipped_duplicate_date"
                        continue

                    schema = self.config.get(table_name, [])
                    if len(schema) == 0:
                        raise Exception(f"정의된 BigQuery 스키마가 없습니다")

                    await self.client.insert_start(data_set_name, table_name, schema, data)
                    result[table_name] = True
                    logger.info(f"{table_name} 데이터 BigQuery 삽입 완료")

                except Exception as e:
                    logger.error(f"{table_name} BigQuery 삽입 실패: {str(e)}")
                    result[table_name] = False
            else:
                logger.warning(f"{table_name} 데이터가 비어있음")

        return result

    async def insert_daynamic_schema(self, data_set_name: str, reports_data: dict) -> dict:
        """동적 스키마를 가진 데이터를 BigQuery에 삽입"""
        result = {}

        await self.client.create_dataset(data_set_name)

        for table_name, data in reports_data.items():

            # 테이블명에서 스키마명 추출 (prefix 기반)
            if table_name.startswith("GA4"):
                schema_name = "GA4"
            elif table_name.startswith("GOOGLE_ADS"):
                schema_name = "GOOGLE_ADS"
            else:
                schema_name = table_name

            basic_schema = self.config[schema_name]

            schema = self._create_schema(basic_schema, data)
            result[table_name] = False

            if data:  # 데이터가 있으면
                try:
                    # 날짜 필드명 결정
                    date_field = 'segments_date' if "GOOGLE_ADS" in table_name else 'date'

                    # GA4 테이블인 경우: 날짜 범위로 삭제 후 삽입
                    if table_name.startswith("GA4"):
                        # 모든 날짜 추출 (리스트)
                        all_dates = [row[date_field] for row in data if date_field in row]
                        if all_dates:
                            start_date = min(all_dates)
                            end_date = max(all_dates)
                            logger.info(f"{table_name}: 날짜 범위 {start_date} ~ {end_date} 데이터 삭제 후 재삽입합니다.")
                            await self.client.delete_data_by_date_range(data_set_name, table_name, start_date, end_date)

                    # 다른 테이블인 경우: 날짜 중복 체크 (첫 번째 날짜만 확인)
                    else:
                        first_date = data[0][date_field]
                        if await self.client.check_date_exists(data_set_name, table_name, first_date, date_field):
                            logger.warning(f"{table_name}: 해당 날짜({first_date})의 데이터가 이미 존재합니다. 삽입을 취소합니다.")
                            result[table_name] = "skipped_duplicate_date"
                            continue

                    if len(schema) == 0:
                        raise Exception(f"생성된 BigQuery 스키마가 없습니다")

                    await self.client.insert_start(data_set_name, table_name, schema, data)
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

    async def get_data_by_date(self, dataset_id: str, table_id: str, 
                               start_date: str, end_date: str):
        """특정 테이블의 특정 날짜 데이터를 조회"""
        try:
            data = await self.client.query_data_by_date(dataset_id, table_id, start_date, end_date)
            result = []
            
            # 결과를 딕셔너리 리스트로 변환
            for row in data:
                result.append(dict(row))

            logger.info(f"Retrieved {len(result)} rows from {dataset_id}.{table_id} for date {start_date} - {end_date}")

            return result
        except Exception as e:
            logger.error(f"데이터 조회 실패: {str(e)}")
            raise e
