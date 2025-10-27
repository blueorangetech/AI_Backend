from utils.http_client_manager import get_http_client
from typing import Dict, Any, List
import logging, asyncio
import os

logger = logging.getLogger(__name__)

class WorksAPIClient:
    def __init__(self, access_token: str):
        self.access_token = access_token

    async def _make_request(self, method, params = None):
        url = "https://www.worksapis.com/v1.0/users/me/mail"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        try:
            if method.upper() == "POST":
                client = await get_http_client()
                response = await client.post(url, headers=headers, json=params)

                if response.status_code in [200, 201, 202]:
                    logger.info(f"Mail sent successfully: {response.status_code}")
                    return {"status": "success", "status_code": response.status_code}
                else:
                    logger.error(f"POST request failed: {response.status_code} - {response.text}")
                    return {"error": f"Status {response.status_code}: {response.text}"}

            else:
                raise ValueError(f"지원하지 않는 요청입니다 : {method}")

        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            return {"error": str(e)}

    
    async def send_mail(self, content):
        response = await self._make_request("POST", content)
        return response