import httpx

class KakaoAPIClient:
    def __init__(self, token, account_id):
        self.token = token
        self.account_id = account_id
        self.client = httpx.AsyncClient()

    
    async def _make_request(self, method, url, params = None):
        headers = {"Authorization": f"Bearer {self.token}","adAccountId": self.account_id}

        if method.upper() == "GET":
            response = await self.client.get(url, headers=headers, params=params)
        
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
    
    async def get_report(self, campaign_id, start_date, end_date):
        url = "https://api.keywordad.kakao.com/openapi/v1/keywords/report"
        params = {"campaignId": campaign_id, "metricsGroups": "BASIC", 
                  "start": start_date, "end": end_date, "timeUnit": "DAY"}

        response = await self._make_request("GET", url, params)
        return response
    
    