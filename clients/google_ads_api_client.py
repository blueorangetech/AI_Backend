# google_ads_client.py
import os
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

## 단일파일 테스트용 라인 시작 ##

def get_google_client(customer_id):
    """Google Ads 클라이언트 생성"""
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
        "use_proto_plus": True
    }
    
    # 필수 환경 변수 검증
    for key, value in config.items():
        if not value:
            raise ValueError(f"{key.upper()} 환경 변수가 설정되지 않았습니다.")
    
    return GoogleAdsAPIClient(**config, customer_id = customer_id)

## 단일파일 테스트용 라인 종료 ##

class GoogleAdsAPIClient:
    def __init__(self, developer_token, client_id, client_secret, 
                 refresh_token, login_customer_id, use_proto_plus, customer_id):
        
        self.customer_id = customer_id
        self.config = {
            'developer_token': developer_token,
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
            "login_customer_id": login_customer_id,
            'use_proto_plus': use_proto_plus
        }
        
        self.client = GoogleAdsClient.load_from_dict(self.config)
    
    def create_report(self):
        ga_service = self.client.get_service("GoogleAdsService")
        query = """
                SELECT 
                    customer.id,
                    customer.descriptive_name,
                    campaign.id,
                    campaign.name,
                    campaign.status,
                    ad_group.id,
                    ad_group.name,
                    ad_group_criterion.keyword.text,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros
                FROM keyword_view
                WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0
        """
        response = ga_service.search(customer_id = self.customer_id, query = query)
        reports = []
        for row in response:
            data = {"customer_id" : row.customer.id,
                    "customer_name": row.customer.descriptive_name,
                    "campaign_id" : row.campaign.id,
                    "campaign_name" : row.campaign.name,
                    "ad_group_id" : row.ad_group.id,
                    "ad_group_name" : row.ad_group.name,
                    "keyword" : row.ad_group_criterion.keyword.text,
                    "imp" : row.metrics.impressions,
                    "click" : row.metrics.clicks,
                    "cost": row.metrics.cost_micros
                    }
            
            reports.append(data)
        
        return reports
        

# 사용 예제
if __name__ == "__main__":
    try:
        # Google Ads 매니저 생성
        ads_manager = get_google_client("2922554629")
        ads_manager.create_report()
        # 연결 테스트
        # if ads_manager.test_connection():
        #     print("\n📊 캠페인 목록:")
        #     campaigns = ads_manager.get_campaigns()
            
        #     if campaigns:
        #         for campaign in campaigns:
        #             print(f"- {campaign['name']} (ID: {campaign['id']}, 상태: {campaign['status']})")
        #     else:
        #         print("캠페인이 없습니다.")
        
    except ValueError as e:
        print(f"설정 오류: {e}")
    except Exception as e:
        print(f"예상치 못한 오류: {e}")