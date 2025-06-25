from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension

class GA4APIClient:

    def __init__(self, config):
        self.config = config
        self.client = BetaAnalyticsDataClient.from_service_account_info(config)


    def request_run_report(self, property_id, event_list):
        default_dimensions = [Dimension(name="sessionSourceMedium"),
                              Dimension(name="sessionCampaignName")]
        
        custom_dimensions = [Dimension(name=f"customEvent:{event}") for event in event_list]
        target_dimensions = default_dimensions + custom_dimensions

        request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        dimensions=[target_dimensions],
        metrics=[Metric(name="eventCount"),]
        )

        response = self.client.run_report(request)

        for row in response.rows:
            print(f"Media / Source: {row.dimension_values[0].value}, Event_Count: {row.metric_values[0].value}")

