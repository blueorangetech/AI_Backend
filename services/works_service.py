from clients.works_api_client import WorksAPIClient
import pandas as pd
import asyncio, logging

logger = logging.getLogger(__name__)

class WorksService:
    def __init__(self, access_token: str):
        self.client = WorksAPIClient(access_token)

    async def send_mail(self, content):
        response = await self.client.send_mail(content)
        return response

