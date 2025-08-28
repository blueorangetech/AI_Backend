from clients.google_ads_api_client import GoogleAdsAPIClient
from models.bigquery_schemas import google_ads_schema
import logging

logger = logging.getLogger(__name__)

class GoogleAdsReportServices:

    def __init__(self, google_client: GoogleAdsAPIClient):
        self.client = google_client
    
    def create_reports(self, fields):
        response = self.client.create_report(fields)
        logger.info(response)
        field_list = [field.strip() for field in fields.split(",")]

        reports = []
        for row in response:
            logger.info(row)
            data = {}
            for field in field_list:
                dict_key = field.replace(".", "_")
                data[dict_key] = self._get_nested_value(row, field)
            
            reports.append(data)
        
        result = {"google_ads": reports}
        return result

    def _get_nested_value(self, obj, attr_path):
        """중첩된 속성 접근"""
        try:
            parts = attr_path.split('.')
            current = obj
            
            for part in parts:
                current = getattr(current, part)
            
            return current
        except AttributeError:
            return None
    