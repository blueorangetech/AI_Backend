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
        """GA4 탐색 보고서 생성"""

        # 필드명 정의
        default_dimensions = [Dimension(name=default) for default in defaults]

        # 커스텀 이벤트가 있으면 eventName dimension 추가
        if customs:
            default_dimensions.append(Dimension(name="eventName"))

        request_params = {
            "property": f"properties/{self.property_id}",
            "date_ranges": [DateRange(start_date="yesterday", end_date="yesterday")],
            "dimensions": default_dimensions,
            "metrics": [Metric(name=metric) for metric in metrics],
        }

        # customs가 있을 때만 필터 추가
        if customs:
            request_params["dimension_filter"] = FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    in_list_filter=Filter.InListFilter(values=customs),
                )
            )

        request = RunReportRequest(**request_params)

        response = self.client.run_report(request)
        return response

    def check_events(self):
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="eventName"),
            ],
            metrics=[
                Metric(name="eventCount"),  # 이벤트 발생 횟수
            ],
            date_ranges=[{"start_date": "30daysAgo", "end_date": "today"}],  # 최근 30일
        )

        response = self.client.run_report(request=request)
        event_names = []
        for row in response.rows:
            event_name = row.dimension_values[0].value
            event_count = row.metric_values[0].value
            event_names.append({"event_name": event_name, "count": int(event_count)})
            print(f"이벤트명: {event_name}, 발생횟수: {event_count}")

        return event_names

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
