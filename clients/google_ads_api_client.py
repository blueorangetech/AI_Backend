from google.ads.googleads.client import GoogleAdsClient
import logging

logger = logging.getLogger(__name__)


class GoogleAdsAPIClient:
    def __init__(self, config, customer_id):
        self.customer_id = customer_id
        self.config = config
        self.client = GoogleAdsClient.load_from_dict(self.config)

    def test_api_connection(self):
        """Google Ads API 연결 테스트"""
        try:
            customer_service = self.client.get_service("CustomerService")
            customer = customer_service.get_customer(
                resource_name=f"customers/{self.customer_id}"
            )

            logger.info(f"API 연결 성공 - 고객: {customer.descriptive_name}")
            return {
                "valid": True,
                "customer_id": customer.id,
                "customer_name": customer.descriptive_name,
                "currency_code": customer.currency_code,
            }

        except Exception as e:
            logger.error(f"API 연결 실패: {e}")
            return {"valid": False, "error": str(e)}

    def create_report(self, fields, view_level, conditions):
        ga_service = self.client.get_service("GoogleAdsService")
        selected_fields = ",".join(fields)
        query = f"""
                SELECT
                {selected_fields}
                {view_level}
                {conditions}
        """
        response = ga_service.search(customer_id=self.customer_id, query=query)
        return response
