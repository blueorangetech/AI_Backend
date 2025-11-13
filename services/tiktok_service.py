from clients.tiktok_api_client import TikTokAPIClient
import pandas as pd
import time, datetime, logging

logger = logging.getLogger(__name__)


class TikTokReportService:
    def __init__(self, tiktoc_client: TikTokAPIClient):
        self.client = tiktoc_client

    async def create_report(self):
        return