from datetime import datetime, timedelta
import httpx
from utils.http_client_manager import get_http_client


class KakaoAPIClient:
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id

    async def _make_request(self, method, url, params=None):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "adAccountId": self.account_id,
        }

        if method.upper() == "GET":
            client = await get_http_client()
            response = await client.get(url, headers=headers, params=params)

        else:
            raise ValueError(f"지원하지 않는 요청입니다: {method}")

        response.raise_for_status()
        return response.json()

    async def get_clients(self):
        # 광고 계정 ID 반환
        url = "https://api.keywordad.kakao.com/openapi/v1/adAccounts/pages"

        response = await self._make_request("GET", url)
        return response

    async def get_campaigns_info(self):
        url = "https://api.keywordad.kakao.com/openapi/v1/campaigns"
        response = await self._make_request("GET", url)
        campaigns_info = {}

        for res in response:
            campaigns_info[res["id"]] = res["name"]

        return campaigns_info

    async def get_groups_info(self, campaign_id):
        url = "https://api.keywordad.kakao.com/openapi/v1/adGroups"
        params = {"campaignId": campaign_id}

        response = await self._make_request("GET", url, params)
        groups_info = {}

        for res in response:
            groups_info[res["id"]] = res["name"]

        return groups_info

    async def get_keywords_info(self, group_id):
        url = "https://api.keywordad.kakao.com/openapi/v1/keywords"
        params = {"adGroupId": group_id}

        response = await self._make_request("GET", url, params)
        keywords_info = {}

        for res in response:
            keywords_info[res["id"]] = res["text"]

        return keywords_info

    async def get_report(self, campaign_id):
        url = "https://api.keywordad.kakao.com/openapi/v1/keywords/report"
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        params = {
            "campaignId": campaign_id,
            "metricsGroups": "BASIC,ADDITION",
            "start": yesterday,
            "end": yesterday,
            "timeUnit": "DAY",
        }

        response = await self._make_request("GET", url, params)
        return response

    # Kakao Moment
    async def get_moment_campaigns_info(self):
        url = "https://apis.moment.kakao.com/openapi/v4/campaigns"
        response = await self._make_request("GET", url)
        camgaigns = {}

        for res in response["content"]:
            camgaigns[str(res["id"])] = res["name"]

        return camgaigns

    async def get_moment_groups_info(self, campaign):
        url = f"https://apis.moment.kakao.com/openapi/v4/adGroups?campaignId={campaign}"
        response = await self._make_request("GET", url)
        groups = {"name": {}, "campaign": {}}

        for res in response["content"]:
            groups["name"][str(res["id"])] = res["name"]
            groups["campaign"][str(res["id"])] = str(campaign)

        return groups

    async def get_moment_creatives_info(self, group):
        url = f"https://apis.moment.kakao.com/openapi/v4/creatives?adGroupId={group}"
        response = await self._make_request("GET", url)
        creatives = {"name": {}, "group": {}}

        for res in response["content"]:
            creatives["name"][str(res["id"])] = res["name"]
            creatives["group"][str(res["id"])] = str(group)

        return creatives

    async def get_moment_report(self, creatives):
        url = "https://apis.moment.kakao.com/openapi/v4/creatives/report"
        params = f"?datePreset=TODAY&dimension=CREATIVE_FORMAT&metricsGroup=BASIC&creativeId={creatives}"

        response = await self._make_request("GET", url + params)
        return response["data"]
