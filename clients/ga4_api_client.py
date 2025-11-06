from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Metric,
    Dimension,
    Filter,
    FilterExpression,
)
import logging

logger = logging.getLogger(__name__)


class GA4APIClient:
    def __init__(self, config, property_id):
        self.config = config
        self.client = BetaAnalyticsDataClient.from_service_account_info(config)
        self.admin = AnalyticsAdminServiceClient.from_service_account_info(config)
        self.property_id = property_id

    def request_create_report(self, defaults, customs, metrics):
        """GA4 탐색 보고서 생성 (페이지네이션 포함)"""

        # 필드명 정의
        default_dimensions = [Dimension(name=default) for default in defaults]

        # 커스텀 이벤트가 있으면 eventName dimension 추가
        if customs:
            default_dimensions.append(Dimension(name="eventName"))

        request_params = {
            "property": f"properties/{self.property_id}",
            "date_ranges": [DateRange(start_date="7daysAgo", end_date="yesterday")],
            "dimensions": default_dimensions,
            "metrics": [Metric(name=metric) for metric in metrics],
            "limit": 100000,  # 최대 limit 설정
        }

        # customs가 있을 때만 필터 추가
        if customs:
            request_params["dimension_filter"] = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=customs),
                )
            )

        # 페이지네이션을 통해 모든 데이터 수집
        all_rows = []
        offset = 0
        limit = 100000

        while True:
            request_params["offset"] = offset
            request_params["limit"] = limit

            request = RunReportRequest(**request_params)
            response = self.client.run_report(request)

            # 데이터가 없으면 종료
            if not response.rows:
                break

            # 수집된 rows 추가
            all_rows.extend(response.rows)
            rows_count = len(response.rows)

            logger.info(f"Fetched {rows_count} rows (offset: {offset}, total: {len(all_rows)})")

            # 반환된 row 개수가 limit보다 작으면 마지막 페이지
            if rows_count < limit:
                break

            offset += limit

        logger.info(f"Total rows fetched: {len(all_rows)}")

        # 마지막 response의 rows를 전체 수집된 rows로 교체
        if all_rows:
            # protobuf repeated field 클리어 후 재추가
            del response.rows[:]
            response.rows.extend(all_rows)

        return response

    def properties_list(self):
        response = self.admin.list_accounts()

        properties = []
        for property in response:
            logger.info(property)
            properties.append({
                "account_id": property.name,
                "display_name": property.display_name
                }
            )

        return properties

    def get_metadata(self):
        """GA4 속성의 사용가능한 차원과 측정항목 조회"""
        from google.analytics.data_v1beta.types import GetMetadataRequest

        request = GetMetadataRequest(
            name=f"properties/{self.property_id}/metadata"
        )

        response = self.client.get_metadata(request)

        # 차원(Dimensions) 정리
        dimensions = []
        for dimension in response.dimensions:
            dimensions.append({
                "api_name": dimension.api_name,
                "ui_name": dimension.ui_name,
                "description": dimension.description,
                "custom_definition": dimension.custom_definition,
                "category": dimension.category
            })

        # 측정항목(Metrics) 정리
        metrics = []
        for metric in response.metrics:
            metrics.append({
                "api_name": metric.api_name,
                "ui_name": metric.ui_name,
                "description": metric.description,
                "custom_definition": metric.custom_definition,
                "category": metric.category
            })

        return {
            "dimensions": dimensions,
            "metrics": metrics
        }
