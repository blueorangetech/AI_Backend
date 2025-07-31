from clients.meta_ads_api_client import MetaAdsAPIClient
import os

def get_meta_ads_client(ad_account_id):
    """Meta Ads 클라이언트 생성"""
    config = {
        "access_token": os.environ["META_ACCESS_TOKEN"],
        "app_id": os.environ["META_APP_ID"],
        "app_secret": os.environ["META_APP_SECRET"]
    }
    
    # 필수 환경 변수 검증
    for key, value in config.items():
        if not value:
            raise ValueError(f"{key.upper()} 환경 변수가 설정되지 않았습니다.")
    
    return MetaAdsAPIClient(**config, ad_account_id=ad_account_id)