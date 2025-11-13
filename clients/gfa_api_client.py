from utils.http_client_manager import get_http_client
from typing import Dict, Any
import logging, asyncio

logger = logging.getLogger(__name__)

class GFAAPIClient:
    def __init__(self, base_url: str, access_token: str, account_no):
        self.base_url = base_url
        self.account_no = account_no
        self.access_token = access_token

    async def _make_request(self, method, uri, retry=5) -> Dict[str, Any]:
        url = self.base_url + uri
        headers = {"Authorization": f"Bearer {self.access_token}"}
        last_error = "Unknown error"

        for attempt in range(retry):
            try:
                if method.upper() == "GET":
                    client = await get_http_client()
                    response = await client.get(url, headers=headers)
                    
                    if response.status_code == 200:
                        return response.json()

                    last_error = f"HTTP {response.status_code}: {response.text}"
                    if attempt < retry - 1:
                        logger.warning(f"Non-200 response ({response.status_code}). Retrying... ({attempt + 1}/{retry})")
                        await asyncio.sleep(1)
                        continue

                else:
                    raise ValueError(f"지원하지 않는 요청입니다 : {method}")

            except Exception as e:
                last_error = str(e)
                if attempt < retry - 1:
                    logger.warning(f"Request failed: {str(e)}. Retrying... ({attempt + 1}/{retry})")
                    continue

        logger.error(f"Max retries reached. Last error: {last_error}")
        return {"status": "error", "message": last_error}

    async def get_manage_accounts(self) -> Dict[str, Any]:
        uri = "/adAccounts"
        response = await self._make_request("GET", uri)
        return response

    async def get_campaigns(self, page: int = 0) -> Dict[str, Any]:
        uri = f"/adAccounts/{self.account_no}/campaigns?page={page}&size=100"
        response = await self._make_request("GET", uri)
        return response

    async def get_adsets(self, page: int = 0) -> Dict[str, Any]:
        uri = f"/adAccounts/{self.account_no}/adSets?page={page}&size=100"
        response = await self._make_request("GET", uri)
        return response

    async def get_creatives(self, page: int = 0) -> Dict[str, Any]:
        uri = f"/adAccounts/{self.account_no}/creatives?page={page}&size=100"
        response = await self._make_request("GET", uri)
        return response

    async def get_performance(self, start_date, end_date) -> Dict[str, Any]:
        uri = f"/adAccounts/{self.account_no}/performance/past/creatives"
        query = f"?startDate={start_date}&endDate={end_date}"
        response = await self._make_request("GET", uri + query)
        return response