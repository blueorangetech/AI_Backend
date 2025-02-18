from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from google.oauth2.service_account import Credentials
import json, os

gcp_info = os.getenv("GCP_INFO")
# print(gcp_info)
key_path = json.loads(gcp_info)
# client = BetaAnalyticsDataClient.from_service_account_file(key_path)

# request = RunReportRequest(
#     property=f"properties/331732082",
#     date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
#     dimensions=[Dimension(name="sessionSourceMedium"),
#                 Dimension(name="sessionCampaignName")
#                 ],

#     metrics=[Metric(name="eventCount"),]
# )

# # 데이터 요청 실행
# response = client.run_report(request)

# for row in response.rows:
#     print(f"도시: {row.dimension_values[0].value}, 활성 사용자: {row.metric_values[0].value}")

# print(response)