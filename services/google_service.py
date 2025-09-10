from clients.google_ads_api_client import GoogleAdsAPIClient
import logging

logger = logging.getLogger(__name__)


class GoogleAdsReportServices:

    def __init__(self, google_client: GoogleAdsAPIClient):
        self.client = google_client

    def create_reports(self, fields, view_level):
        response = self.client.create_report(fields, view_level)

        reports = []
        for row in response:
            data = {}
            for field in fields:
                dict_key = field.replace(".", "_")
                data[dict_key] = self._get_nested_value(row, field)

            reports.append(data)

        result = {"GOOGLE_ADS": reports}
        return result

    def _get_nested_value(self, obj, attr_path):
        """중첩된 속성 접근"""
        try:
            parts = attr_path.split(".")
            current = obj

            for part in parts:
                current = getattr(current, part)

            # enum 타입이면 .name으로 텍스트 값 반환
            if hasattr(current, "name") and hasattr(current, "value"):
                return current.name

            return current
        except AttributeError:
            return None
