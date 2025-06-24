import os
from clients.google_ads_api_client import GoogleAdsAPIClient
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

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