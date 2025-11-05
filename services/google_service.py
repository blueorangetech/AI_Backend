from clients.google_ads_api_client import GoogleAdsAPIClient
import logging

logger = logging.getLogger(__name__)


class GoogleAdsReportServices:

    def __init__(self, google_client: GoogleAdsAPIClient):
        self.client = google_client

    def create_reports(self, data, report_type):
        logger.info(data)
        fields = data.get("fields")
        view_level = data.get("view_level")

        response = self.client.create_report(fields, view_level)

        reports = []
        for row in response:
            data = {}
            for field in fields:
                dict_key = field.replace(".", "_")
                data[dict_key] = self._get_nested_value(row, field)

            reports.append(data)

        result = {f"GOOGLE_ADS_{report_type}": reports}
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

            # final_urls 리스트인 경우 첫 번째 요소만 반환
            if hasattr(current, '__iter__') and not isinstance(current, str) and current:
                return list(current)[0]

            return current
        except AttributeError:
            return None
