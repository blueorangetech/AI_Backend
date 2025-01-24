import requests, json, os
import pandas as pd
# from database.mongodb import MongoClient

# "https://kauth.kakao.com/oauth/authorize?client_id=0b8dfdd0c7e9b669e1b6a6edd9705c92&redirect_uri=http://blorange.net/blormanage/api_restapi_kakao.php&response_type=code"

def generate_token(code, client_id, redirect_uri):
    url = "https://kauth.kakao.com/oauth/token"

    data = {
        "grant_type" : "authorization_code",
        "client_id" : client_id,
        "redirect_uri" : redirect_uri,
        "code" : code
    }

    response = requests.post(url, data=data)
    tokens = response.json()

    if "access_token" in tokens:
        with open("kakao_token.json", "w") as fp:
            json.dump(tokens, fp)
            print("Tokens saved successfully")
    else:
        print(tokens)


def check_token(token):
    url = "https://kapi.kakao.com/v1/user/access_token_info"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    print(response.json())

def renewal_token(refresh_token, client_id):
    url = "https://kauth.kakao.com/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=utf-8"}
    body = {"grant_type": "refresh_token",
            "client_id": client_id, # os.environ 대체
            "refresh_token": refresh_token} #os.environ 대체
    
    response = requests.post(url, headers=headers, json=body)
    print(response.status_code)
    print(response.json())


def get_clients(token):
    # 광고 계정 ID 반환
    account_url = "https://api.keywordad.kakao.com/openapi/v1/adAccounts/pages"

    headers = {"Authorization": f"Bearer {token}"}
    response =requests.get(account_url, headers=headers)
    print(response.json())

def get_campagin_list(token, account_id):
    # 계정 단위 조회
    url = "https://api.keywordad.kakao.com/openapi/v1/campaigns"

    headers = {"Authorization": f"Bearer {token}","adAccountId": account_id}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        campaign_info = {}
        results = response.json()
        for result in results:
            campaign_info[result["id"]] = result["name"]
        
        return campaign_info

def get_group_list():
    url = "https://api.keywordad.kakao.com/openapi/v1/adGroups"
    return

if __name__ == "__main__":
    print()
    