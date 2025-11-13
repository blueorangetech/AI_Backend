from utils.http_client_manager import get_http_client
from typing import Dict, Any, Optional
import logging, os


logger = logging.getLogger(__name__)

class TikTokAPIClient:
    def __init__(self, advertiser_id: str):
        self.base_url = "https://business-api.tiktok.com/open_api/v1.3"
        self.access_token = os.environ["TIKTOK_ACCESS_TOKEN"]
        self.advertiser_id = advertiser_id
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
            "advertiser_id": self.advertiser_id,
            "page_size": page_size,
            "page": page
        }
        return await self._make_request("GET", uri, params=params)

    async def get_adgroups(self, page_size: int = 100, page: int = 1):
        return