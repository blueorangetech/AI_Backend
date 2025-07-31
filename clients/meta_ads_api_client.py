import requests
import logging, dotenv
from datetime import datetime, timedelta

dotenv.load_dotenv()
logger = logging.getLogger(__name__)

class MetaAdsAPIClient:
    def __init__(self, access_token, app_id, app_secret, ad_account_id):
        self.access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret
        self.ad_account_id = ad_account_id
        self.base_url = "https://graph.facebook.com/v23.0"
        
    def _make_request(self, endpoint, params=None):
        """API 요청 공통 함수"""
        if params is None:
            params = {}
        
        params['access_token'] = self.access_token
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Meta API 요청 실패: {e}")
            raise
    
    def get_campaigns(self, date_preset='yesterday'):
        """캠페인 목록과 성과 데이터 조회"""
        endpoint = f"act_{self.ad_account_id}/campaigns"
        
        params = {
            'fields': 'id,name,status,objective,created_time,updated_time',
            'limit': 1000
        }
        
        campaigns_data = self._make_request(endpoint, params)
        
        # 각 캠페인의 성과 데이터 조회
        campaigns_with_insights = []
        for campaign in campaigns_data.get('data', []):
            insights = self.get_campaign_insights(campaign['id'], date_preset)
            campaign['insights'] = insights
            campaigns_with_insights.append(campaign)
            
        return campaigns_with_insights
    
    def get_campaign_insights(self, campaign_id, date_preset='yesterday'):
        """캠페인 성과 데이터 조회"""
        endpoint = f"{campaign_id}/insights"
        
        params = {
            'date_preset': date_preset,
            'fields': 'impressions,clicks,spend,ctr,cpm,cpp,reach,frequency,actions',
            'level': 'campaign'
        }
        
        insights_data = self._make_request(endpoint, params)
        return insights_data.get('data', [])
    
    def get_adsets(self, campaign_id=None, date_preset='yesterday'):
        """광고세트 목록과 성과 데이터 조회"""
        if campaign_id:
            endpoint = f"{campaign_id}/adsets"
        else:
            endpoint = f"act_{self.ad_account_id}/adsets"
        
        params = {
            'fields': 'id,name,status,campaign_id,targeting,created_time,updated_time',
            'limit': 1000
        }
        
        adsets_data = self._make_request(endpoint, params)
        
        # 각 광고세트의 성과 데이터 조회
        adsets_with_insights = []
        for adset in adsets_data.get('data', []):
            insights = self.get_adset_insights(adset['id'], date_preset)
            adset['insights'] = insights
            adsets_with_insights.append(adset)
            
        return adsets_with_insights
    
    def get_adset_insights(self, adset_id, date_preset='yesterday'):
        """광고세트 성과 데이터 조회"""
        endpoint = f"{adset_id}/insights"
        
        params = {
            'date_preset': date_preset,
            'fields': 'impressions,clicks,spend,ctr,cpm,cpp,reach,frequency,actions',
            'level': 'adset'
        }
        
        insights_data = self._make_request(endpoint, params)
        return insights_data.get('data', [])
    
    def get_ads(self, adset_id=None, date_preset='yesterday'):
        """광고 목록과 성과 데이터 조회"""
        if adset_id:
            endpoint = f"{adset_id}/ads"
        else:
            endpoint = f"act_{self.ad_account_id}/ads"
        
        params = {
            'fields': 'id,name,status,adset_id,campaign_id,created_time,updated_time',
            'limit': 1000
        }
        
        ads_data = self._make_request(endpoint, params)
        
        # 각 광고의 성과 데이터 조회
        ads_with_insights = []
        for ad in ads_data.get('data', []):
            insights = self.get_ad_insights(ad['id'], date_preset)
            ad['insights'] = insights
            ads_with_insights.append(ad)
            
        return ads_with_insights
    
    def get_ad_insights(self, ad_id, date_preset='yesterday'):
        """광고 성과 데이터 조회"""
        endpoint = f"{ad_id}/insights"
        
        params = {
            'date_preset': date_preset,
            'fields': 'impressions,clicks,spend,ctr,cpm,cpp,reach,frequency,actions,video_play_actions',
            'level': 'ad'
        }
        
        insights_data = self._make_request(endpoint, params)
        return insights_data.get('data', [])
    
    def get_account_insights(self, date_preset='yesterday', breakdowns=None):
        """광고 계정 전체 성과 데이터 조회"""
        endpoint = f"act_{self.ad_account_id}/insights"
        
        params = {
            'date_preset': date_preset,
            'fields': 'impressions,clicks,spend,ctr,cpm,cpp,reach,frequency,actions,conversions',
            'level': 'account'
        }
        
        if breakdowns:
            params['breakdowns'] = ','.join(breakdowns)
        
        insights_data = self._make_request(endpoint, params)
        return insights_data.get('data', [])
    
    def get_custom_insights(self, time_range, breakdowns=None, fields=None):
        """커스텀 기간 성과 데이터 조회"""
        endpoint = f"act_{self.ad_account_id}/insights"
        
        default_fields = 'impressions,clicks,spend,ctr,cpm,cpp,reach,frequency,actions'
        
        params = {
            'time_range': time_range,  # {'since': 'YYYY-MM-DD', 'until': 'YYYY-MM-DD'}
            'fields': fields or default_fields,
            'level': 'ad'
        }
        
        if breakdowns:
            params['breakdowns'] = ','.join(breakdowns)
        
        insights_data = self._make_request(endpoint, params)
        return insights_data.get('data', [])
    
    def get_ad_accounts(self):
        """권한이 있는 광고 계정 목록 조회"""
        endpoint = "me/adaccounts"
        
        params = {
            'fields': 'id,name,account_status,currency,timezone_name,business,account_id',
            'limit': 1000
        }
        
        try:
            result = self._make_request(endpoint, params)
            accounts = result.get('data', [])
            
            logger.info(f"접근 가능한 광고 계정 수: {len(accounts)}")
            for account in accounts:
                logger.info(f"계정: {account.get('name')} (ID: {account.get('id')}, 상태: {account.get('account_status')})")
            
            return accounts
        except Exception as e:
            logger.error(f"광고 계정 목록 조회 실패: {e}")
            return []
    
    def get_businesses(self):
        """권한이 있는 비즈니스 목록 조회"""
        endpoint = "me/businesses"
        
        params = {
            'fields': 'id,name,verification_status,permitted_tasks',
            'limit': 1000
        }
        
        try:
            result = self._make_request(endpoint, params)
            businesses = result.get('data', [])
            
            logger.info(f"접근 가능한 비즈니스 수: {len(businesses)}")
            for business in businesses:
                logger.info(f"비즈니스: {business.get('name')} (ID: {business.get('id')})")
            
            return businesses
        except Exception as e:
            logger.error(f"비즈니스 목록 조회 실패: {e}")
            return []

    def test_connection(self):
        """연결 테스트"""
        try:
            endpoint = f"me?"
            params = {'fields': 'id,name'}
            
            result = self._make_request(endpoint, params)
            logger.info(f"Meta 계정 연결 성공: {result.get('name')}")
            return True
        except Exception as e:
            logger.error(f"Meta 계정 연결 실패: {e}")
            return False

# 사용 예제
if __name__ == "__main__":
    import os
    
    # 환경 변수에서 설정값 로드
    access_token = os.environ.get("META_ACCESS_TOKEN")
    app_id = os.environ.get("META_APP_ID")
    app_secret = os.environ.get("META_APP_SECRET")
    ad_account_id = "751842441589787"
    
    if not all([access_token, app_id, app_secret, ad_account_id]):
        print("환경 변수를 설정해주세요: META_ACCESS_TOKEN, META_APP_ID, META_APP_SECRET, META_AD_ACCOUNT_ID")
        exit(1)
    
    # Meta Ads 클라이언트 생성
    meta_client = MetaAdsAPIClient(access_token, app_id, app_secret, ad_account_id)
    meta_client.get_businesses()

    # 연결 테스트
    if meta_client.test_connection():
        businesses = meta_client.get_businesses()
        for bus in businesses:
            print(bus)

        # print("📊 어제 캠페인 성과:")
        # campaigns = meta_client.get_campaigns('yesterday')
        
        # for campaign in campaigns:
        #     print(f"캠페인: {campaign['name']}")
        #     for insight in campaign.get('insights', []):
        #         print(f"  - 노출: {insight.get('impressions', 0)}")
        #         print(f"  - 클릭: {insight.get('clicks', 0)}")
        #         print(f"  - 비용: ${insight.get('spend', 0)}")