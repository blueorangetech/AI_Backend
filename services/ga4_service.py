from clients.ga4_api_client import GA4APIClient
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class GA4ReportServices:

    def __init__(self, google_client: GA4APIClient):
        self.client = google_client

    def create_report(self, data, report_type):
        defaults = data.get("default", [])
        metrics = data.get("metric", [])
        event_filters = data.get("filter", [])
        start = data.get("date_range", "7daysAgo")
        
        response = self.client.request_create_report(defaults, metrics, event_filters, start)
        if event_filters:
            dimensions = defaults + ["eventName"]

        else:
            dimensions = defaults

        data = []
        for rows in response.rows:
            result = {}
            for row, header in zip(rows.dimension_values, dimensions):
                result[header] = row.value

            for row, metric in zip(rows.metric_values, metrics):
                if "keyEvents" in metrics:
                    result[metric] = float(row.value)

                else:
                    result[metric] = int(row.value)

            data.append(result)
        
        data_df = pd.DataFrame(data)
        data_df["date"] = pd.to_datetime(data_df["date"], format="%Y%m%d").dt.strftime(
            "%Y-%m-%d"
        )
        data = data_df.to_dict("records")
        results = {f"GA4_{report_type}": data}
        return results

    def properties_list(self):
        property_list = self.client.properties_list()
        return property_list

    def get_metadata(self):
        """사용가능한 차원과 측정항목 조회"""
        metadata = self.client.get_metadata()
        return metadata
