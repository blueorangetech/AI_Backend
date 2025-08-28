import hashlib, time, hmac, base64
import httpx
from datetime import datetime, timedelta

class NaverAPIClient:

    def __init__(self, base_url: str, api_key: str, secret_key: str, customer_id: str):
        self.base_url = base_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.customer_id = customer_id
        self.client = httpx.AsyncClient()

    def _generate_signature(self, timestamp, method, uri):
        """ 서명 생성 """
        message = "{}.{}.{}".format(timestamp, method, uri)
        hash = hmac.new(
            bytes(self.secret_key.encode('utf-8')), 
            bytes(message.encode('utf-8')), 
            hashlib.sha256
            )

        hash.hexdigest()
        return base64.b64encode(hash.digest())

    def _create_headers(self, method, uri):
        """ API 호출용용 헤더 생성 """
        timestamp = str(round(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, uri)
        return {
            'Content-Type': 'application/json; charset=UTF-8', 
            'X-Timestamp': timestamp, 
            'X-API-KEY': self.api_key, 
            'X-Customer': str(self.customer_id), 
            'X-Signature': signature
            }

    async def _make_request(self, method, uri, data = None, download_url = None):
        """ API 요청 실행 """
        headers = self._create_headers(method, uri)
        
        # 다운로드 URL은 baseURL + URI 조합이 아님
        url = download_url if download_url is not None else self.base_url + uri

        if method.upper() == "POST":
            response = await self.client.post(url, headers=headers, json=data)
        
        elif method.upper() == "GET":
            response = await self.client.get(url, headers=headers)

        elif method.upper() == "DELETE":
            response = await self.client.delete(url, headers=headers)

        else:
            raise ValueError(f"지원하시 않는 요청입니다 : {method}")
        
        response.raise_for_status()
        return response

    async def create_stat_report(self, report_type):
        """ 통계 리포트 생성 API """
        uri = "/stat-reports"
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        data = {"reportTp": report_type, "statDt": yesterday}

        response = await self._make_request("POST", uri, data)
        return response.json()["reportJobId"]
    
    async def create_master_report(self, item_type):
        """ 마스터 리포트 생성 API """
        uri = "/master-reports"
        data = {"item": item_type}

        response = await self._make_request("POST", uri, data)
        return response.json()["id"]
    
    async def get_report_status(self, uri, report_id):
        """ 리포트 상태 조회"""
        uri = f"{uri}/{report_id}"
        response = await self._make_request("GET", uri)
        return response.json()
    
    async def request_download_url(self, download_url):
        uri = "/report-download"
        response = await self._make_request("GET", uri, download_url=download_url)
        return response
    
    async def delete_report(self, uri, report_id):
        uri = f"{uri}/{report_id}"
        response = await self._make_request("DELETE", uri)
        return response