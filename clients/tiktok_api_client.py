from utils.http_client_manager import get_http_client
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging, os, json


logger = logging.getLogger(__name__)

class TikTokAPIClient:
    def __init__(self, account_id: str):
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
        self.access_token = os.environ["TIKTOK_ACCESS_TOKEN"]
        self.account_id = account_id
        self.headers = {
            "Access-Token": self.access_token,
            "Content-Type": "application/json"
        }

    async def _make_request(self, method: str, uri: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = self.base_url + uri
        client = await get_http_client()
        
        try:
            if method.upper() == "GET":
                response = await client.get(url, headers=self.headers, params=params)

            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            return response.json()

        except Exception as e:
            logger.error(f"TikTok API request failed: {e}")
            return {"error": str(e)}

    async def get_campaigns(self, page_size: int = 100, page: int = 1) -> Dict[str, Any]:
        uri = "/campaign/get/"
        params = {
            "account_id": self.account_id,
            "page_size": page_size,
            "page": page
        }
        return await self._make_request("GET", uri, params=params)

    async def get_adgroups(self, page_size: int = 100, page: int = 1):
        return
    
    async def get_reports(self, page: int = 1, page_size: int = 100,
                          dimensions_list: list = [], metrics_list: list = []):
        uri = "/report/integrated/get/"

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        params = {
            "advertiser_id": self.account_id,
            "service_type": "AUCTION",
            "data_level": "AUCTION_AD",
            "report_type": "BASIC",
            "dimensions" : json.dumps(dimensions_list),
            "metrics": json.dumps(metrics_list),
            "start_date": yesterday,
            "end_date": yesterday,
            "page": page,
            "page_size": page_size
            }
        
        response = await self._make_request("GET", uri, params=params)

        return response
    