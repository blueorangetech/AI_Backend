from functools import lru_cache
from clients.naver_api_client import NaverAPIClient
from clients.gfa_api_client import GFAAPIClient
import os


@lru_cache
def get_naver_client(customer_id: str):
    base_config = {
        "base_url": "https://api.searchad.naver.com",
        "api_key": os.environ["NAVER_API_KEY"],
        "secret_key": os.environ["NAVER_SECRET_KEY"],
    }

    return NaverAPIClient(**base_config, customer_id=customer_id)

def get_gfa_client():
    base_config = {
        "base_url": "https://openapi.naver.com/v1/ad-api/1",
        "access_token": os.environ["GFA_ACCESS_TOKEN"],
        "refresh_token": os.environ["GFA_REFRESH_TOKEN"],
    }
    return GFAAPIClient(**base_config)