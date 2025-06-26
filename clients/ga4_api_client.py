from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension

class GA4APIClient:
    def __init__(self, config, property_id):
        self.config = config
        self.client = BetaAnalyticsDataClient.from_service_account_info(config)
        self.admin = AnalyticsAdminServiceClient.from_service_account_info(config)
        self.property_id = property_id

    def request_run_report(self, event_list):
        default_dimensions = [Dimension(name="sessionSourceMedium"),
                              Dimension(name="sessionCampaignName")]
        
        custom_dimensions = [Dimension(name=f"customEvent:{event}") for event in event_list]
        target_dimensions = default_dimensions + custom_dimensions

        request = RunReportRequest(
        property=f"properties/{self.property_id}",
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        dimensions=[target_dimensions],
        metrics=[Metric(name="eventCount"),]
        )

        response = self.client.run_report(request)

        for row in response.rows:
            print(f"Media / Source: {row.dimension_values[0].value}, Event_Count: {row.metric_values[0].value}")
    
    def check_events(self):
        request = RunReportRequest(
        property=f"properties/{self.property_id}",
        dimensions=[
            Dimension(name="eventName"),
        ],
        metrics=[
            Metric(name="eventCount"),  # 이벤트 발생 횟수
        ],
        date_ranges=[{
            'start_date': '30daysAgo',  # 최근 30일
            'end_date': 'today'
        }]
        )

        response = self.client.run_report(request=request)
        event_names = []
        for row in response.rows:
            event_name = row.dimension_values[0].value
            event_count = row.metric_values[0].value
            event_names.append({
                'event_name': event_name,
                'count': int(event_count)
            })
            print(f"이벤트명: {event_name}, 발생횟수: {event_count}")

        return event_names

    def properties_list(self):
        response = self.admin.list_accounts()
        
        properties = []
        for property in response:
            properties.append(property.display_name)
        
        return properties