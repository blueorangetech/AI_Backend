import os
from clients.ga4_api_client import GA4APIClient

def get_ga4_client():
    """Google Ads 클라이언트 생성"""
    config = {
        "type": os.environ["GA4_TYPE"],
        "project_id": os.environ["GA4_PROJECT_ID"],
        "private_key_id": os.environ["GA4_PRIVATE_KEY_ID"],
        "private_key": os.environ["GA4_PRIVATE_KEY"],
        "client_email": os.environ["GA4_CLIENT_EMAIL"],
        "client_id": os.environ["GA4_CLIENT_ID"],
        "auth_uri": os.environ["GA4_AUTH_URI"],
        "token_uri": os.environ["GA4_TOKEN_URI"],
        "auth_provider_x509_cert_url": os.environ["GA4_AUTH_PROVIDER_X509_CERT_URL"],
        "client_x509_cert_url": os.environ["GA4_CLIENT_X509_CRET_URL"],
        "universe_domain": os.environ["GA4_UNIVERSE_DOMAIN"]
    }
    
    for key, value in config.items():
        if not value:
            raise ValueError(f"{key.upper()} 환경 변수가 설정되지 않았습니다.")
    
    return GA4APIClient(config)