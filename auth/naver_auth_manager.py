from clients.naver_api_client import NaverAPIClient
from clients.gfa_api_client import GFAAPIClient
import os


def get_naver_client(customer_id: str):
    base_config = {
        "base_url": "https://api.searchad.naver.com",
        "api_key": os.environ["NAVER_API_KEY"],
        "secret_key": os.environ["NAVER_SECRET_KEY"],
    }

    return NaverAPIClient(**base_config, customer_id=customer_id)

def get_gfa_client(access_token: str, account_no: str):
    base_config = {
        "base_url": "https://openapi.naver.com/v1/ad-api/1.0",
        "access_token": access_token,
        "account_no": account_no
    }
    return GFAAPIClient(**base_config)