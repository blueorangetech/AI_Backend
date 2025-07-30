from clients.google_ads_api_client import GoogleAdsAPIClient
from google.cloud import bigquery
from models.bigquery_schemas import get_google_ads_schema
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
        
        return reports
    
    def create_schema(self, report):
        if not report:
            return []

        field_index = get_google_ads_schema()

        schema_fields = []

        for key in report[0]:
            if key in field_index:
                data_type = field_index[key]
                schema_fields.append(bigquery.SchemaField(key, data_type))

            else:
                raise Exception(f"Unknown field '{key}' not found in field_index")
            
        return schema_fields

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
    