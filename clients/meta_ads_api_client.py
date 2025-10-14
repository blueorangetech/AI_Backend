import requests
import httpx
import logging, dotenv, json
import pandas as pd
from utils.http_client_manager import get_http_client

dotenv.load_dotenv()
logger = logging.getLogger(__name__)


class MetaAdsAPIClient:
    def __init__(self, access_token, app_id, app_secret, account_id):
        self.access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.account_id = account_id
        self.base_url = "https://graph.facebook.com/v23.0"

    async def _make_request(self, endpoint, params=None):
        """API 요청 공통 함수"""
        if params is None:
            params = {}

        params["access_token"] = self.access_token

        url = f"{self.base_url}/{endpoint}"

        try:
            client = await get_http_client()
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Meta API 요청 실패: {e}")
            raise


    async def verify_account_in_list(self):
        """내 계정 목록에서 특정 계정 찾기"""
        endpoint = "me/adaccounts"

        params = {"fields": "id,name,account_status", "limit": 1000}

        try:
            result = await self._make_request(endpoint, params)
            accounts = result.get("data", [])
            logger.info(accounts)
            for account in accounts:
                if account.get("id") == f"act_{self.account_id}":
                    return True

            return False

        except Exception as e:
            logger.info(f"❌ 오류: {e}")
            return False

    async def get_optimization_goals(self):
        """캠페인과 광고셋의 최적화 목표 조회"""

        # 캠페인 레벨에서 목표 확인
        campaign_fields = ["campaign_id", "campaign_name", "objective", "status"]
        params = {"fields": ",".join(campaign_fields), "limit": 100}
        try:
            # 캠페인 목표 조회
            end_point = f"act_{self.account_id}/campaigns"
            campaign_response = await self._make_request(end_point, params=params)
            campaign_goal = campaign_response.get("data", [])

            return campaign_goal

        except Exception as e:
            return {"error": str(e)}

    async def get_adset_performance(self, fields, start_date=None, end_date=None):
        """광고셋 단위 성과 데이터 조회"""
        all_data = []
        next_cursor = None

        while True:
            params = {
                "access_token": self.access_token,
                "fields": ",".join(fields),
                "level": "ad",  # 광고셋 레벨
                "use_account_attribution_setting": True,
                "limit": 100,
            }

            if start_date and end_date:
                params["time_range"] = json.dumps(
                    {
                        "since": start_date,  # "2024-08-01"
                        "until": end_date,  # "2024-08-05"
                    }
                )

            else:
                params["date_preset"] = "yesterday"

            if next_cursor:
                params["after"] = next_cursor

            try:
                end_point = f"act_{self.account_id}/insights"
                response = await self._make_request(end_point, params)
                all_data.extend(response.get("data", []))

                # 다음 페이지가 있는지 확인
                if "paging" in response and "next" in response["paging"]:
                    next_cursor = response["paging"]["cursors"]["after"]
                else:
                    break  # 더 이상 페이지 없음

            except Exception as e:
                return {"error": str(e)}

        return all_data
