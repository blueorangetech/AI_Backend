import logging
import urllib.parse
from datetime import datetime
from utils.http_client_manager import get_http_client

logger = logging.getLogger(__name__)

class GFAAPIClient:
    def __init__(self, base_url: str, access_token: str, refresh_token: str):
        self.base_url = base_url
        self.access_token = access_token
        self.refresh_token = refresh_token

    async def _make_request(self, method, uri, data=None):
        url = self.base_url + uri
        
        encoded_token = urllib.parse.quote(self.access_token, safe='')
        headers = {"Authorization": f"Bearer {encoded_token}"}
        
        if method.upper() == "GET":
            logger.info("요청 처리중")
            logger.info(f"인코딩된 토큰: {encoded_token}")
            client = await get_http_client()
            response = await client.get(url, headers=headers)
            return response
        
        else:
            raise ValueError(f"지원하시 않는 요청입니다 : {method}")

    async def get_manage_accounts(self):
        uri = "/adAccounts"
        response = await self._make_request("GET", uri)

        return response
    