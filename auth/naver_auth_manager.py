from functools import lru_cache
from clients.naver_api_client import NaverAPIClient
import os

@lru_cache
def get_naver_client(customer_id: str):
    base_config = {
        'base_url': 'https://api.searchad.naver.com',
        'api_key': os.environ["NAVER_API_KEY"],
        'secret_key': os.environ["NAVER_SECRET_KEY"]
    }

    return NaverAPIClient(**base_config, customer_id=customer_id)