import requests, json, os
import pandas as pd
# from database.mongodb import MongoClient

# "https://kauth.kakao.com/oauth/authorize?client_id=0b8dfdd0c7e9b669e1b6a6edd9705c92&redirect_uri=http://blorange.net/lib/api_restapi_kakao.php&response_type=code"

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
    print(tokens)
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
    
    if response.status_code == 200:
        return True
    
    return False

def renewal_token(refresh_token, client_id):
    try:
        url = "https://kauth.kakao.com/oauth/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded;charset=utf-8"}
        body = {"grant_type": "refresh_token",
                "client_id": client_id, # os.environ 대체
                "refresh_token": refresh_token} #os.environ 대체
        
        response = requests.post(url, headers=headers, data=body)
        
        result = response.json()
        if response.status_code == 200:
            new_token = {"access_token": None, "refresh_token": None}

            new_token["access_token"] = result["access_token"]

            if "refresh_token" in result:
                new_token["refresh_token"] = result["refresh_token"]

            return new_token

        raise ValueError(result["error_description"])

    except ValueError as e:
        return {"error": str(e)}

def get_clients(token):
    # 광고 계정 ID 반환
    account_url = "https://api.keywordad.kakao.com/openapi/v1/adAccounts/pages"

    headers = {"Authorization": f"Bearer {token}"}
    response =requests.get(account_url, headers=headers)
    print(response.json())

def get_campagins(token, account_id):
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

def get_groups(token, account_id, campaign_id):
    url = "https://api.keywordad.kakao.com/openapi/v1/adGroups"
    headers = {"Authorization": f"Bearer {token}","adAccountId": account_id}
    params = {"campaignId": campaign_id}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        group_info = {}
        results = response.json()
        
        for result in results:
            group_info[result["id"]] = result["name"]

        return group_info

def get_keywords(token, account_id, group_id):
    url = "https://api.keywordad.kakao.com/openapi/v1/keywords"
    headers = {"Authorization": f"Bearer {token}","adAccountId": account_id}
    params = {"adGroupId": group_id}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        keyword_info = {}
        results = response.json()

        for result in results:
            keyword_info[result["id"]] = result["text"]
        
        return keyword_info
    
def get_reports(token, account_id, campaign_id, start, end):
    url = "https://api.keywordad.kakao.com/openapi/v1/keywords/report"
    headers = {"Authorization": f"Bearer {token}","adAccountId": account_id}
    params = {"campaignId": campaign_id, "metricsGroups": "BASIC", 
              "start": start, "end": end, "timeUnit": "DAY"}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        index = 0
        report_info = {}
        results = response.json()
        
        for result in results["data"]:
            date = result["start"]
            
            campaign = result["dimensions"]["campaignId"]
            group = result["dimensions"]["adGroupId"]
            keyword = result["dimensions"]["keywordId"]

            imp = result["metrics"]["imp"]
            click = result["metrics"]["click"]
            cost = result["metrics"]["spending"]

            report_info[index] = {"date": date, "campaign": campaign, "group": group,
                                  "keyword": keyword, "imp": imp, "click": click, "cost": cost}
            index += 1
        
        table_data = pd.DataFrame.from_dict(report_info, orient="index")
        return table_data

if __name__ == "__main__":
    generate_token("HtXmantwBMZ8aYe_Z9ZkOXg0yd0BK9X2Vp_lTt5NrfFVwtrdwZsNyQAAAAQKFxItAAABl3unLCH_A_o_BVb6-Q", "0b8dfdd0c7e9b669e1b6a6edd9705c92", "http://blorange.net/lib/api_restapi_kakao.php" )
    # 광고주 선택 단계 스킵 - 데모 버전
    # campaigns = get_campagins(token, "627936")
    # print(campaigns)

    # groups = {}

    # reports = pd.DataFrame(columns=["date", "campaign", "group", "keyword", "imp", "click", "cost"])

    # for campaign in campaigns:
    #     report_part = get_reports(token, "627936", campaign, "20241001", "20241002")
    #     reports = pd.concat([reports, report_part], ignore_index=True)
    
    # print(reports)
        
    # for campaign in campaigns:
    #     group_part = get_groups(token, "627936", campaign)
    #     groups = {**groups, **group_part}

    # keywords = {}
    # for group in groups:
    #     keyword_part = get_keywords(token, "627936", group)
    #     keywords = {**keywords, **keyword_part}
    

    # reports["campagin_name"] = reports["campaign"].apply(lambda x: campaigns[x])
    # reports["group_name"] = reports["group"].apply(lambda x: groups[x])
    # reports["keyword_name"] = reports["keyword"].apply(lambda x: keywords[x])

    
    