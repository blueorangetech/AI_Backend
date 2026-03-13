from google.cloud import bigquery


def naver_search_ad_schema():
    """네이버 검색광고 BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaignName": "STRING",
        "campaignID": "STRING",
        "adGroupName": "STRING",
        "adGroupID": "STRING",
        "adKeyword": "STRING",
        "adKeywordID": "STRING",
        "adID": "STRING",
        "pcMobileType": "STRING",
        "impressions": "INTEGER",
        "clicks": "INTEGER",
        "cost": "FLOAT",
        "sumofADrank": "FLOAT",
    }
    return FIELD_TYPE_MAP


def naver_search_ad_cov_schema():
    """네이버 검색광고 전환 BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaignName": "STRING",
        "campaignID": "STRING",
        "adGroupName": "STRING",
        "adGroupID": "STRING",
        "adKeyword": "STRING",
        "adKeywordID": "STRING",
        "adID": "STRING",
        "pcMobileType": "STRING",
        "conversionType": "STRING",
        "conversionCount": "FLOAT",
        "salesByConversion": "INTEGER",
    }
    return FIELD_TYPE_MAP

def naver_shopping_ad_schema():
    """네이버 쇼핑광고 BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaignID": "STRING",
        "campaignName": "STRING",
        "adGroupID": "STRING",
        "adGroupName": "STRING",
        "searchKeyword": "STRING",
        "adID": "STRING",
        "productName": "STRING",
        "productID": "STRING",
        "productIDofMall": "STRING",
        "pcMobileType": "STRING",
        "impressions": "INTEGER",
        "clicks": "INTEGER",
        "cost": "FLOAT",
        "sumofADrank": "FLOAT",
    }
    return FIELD_TYPE_MAP

def naver_shopping_ad_cov_schema():
    """네이버 쇼핑광고 전환 BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaignID": "STRING",
        "campaignName": "STRING",
        "adGroupID": "STRING",
        "adGroupName": "STRING",
        "searchKeyword": "STRING",
        "adID" : "STRING",
        "productName": "STRING",
        "productID": "STRING",
        "productIDofMall": "STRING",
        "pcMobileType": "STRING",
        "conversionType": "STRING",
        "conversionCount": "FLOAT",
        "salesByConversion": "INTEGER"
    }
    return FIELD_TYPE_MAP

def naver_gfa_schema():
    """네이버 GFA BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignNo", "INTEGER"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("adSetNo", "INTEGER"),
        bigquery.SchemaField("adset_name", "STRING"),
        bigquery.SchemaField("creativeNo", "INTEGER"),
        bigquery.SchemaField("creative_name", "STRING"),
        bigquery.SchemaField("impCount", "INTEGER"),
        bigquery.SchemaField("clickCount", "INTEGER"),
        bigquery.SchemaField("vplayCount", "INTEGER"),
        bigquery.SchemaField("sales", "FLOAT"),
        bigquery.SchemaField("convCount", "INTEGER"),
        bigquery.SchemaField("convSales", "FLOAT"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
    ]

def kakao_search_ad_schema():
    """카카오 검색광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("groupID", "STRING"),
        bigquery.SchemaField("groupName", "STRING"),
        bigquery.SchemaField("keywordID", "STRING"),
        bigquery.SchemaField("keywordName", "STRING"),
        bigquery.SchemaField("imp", "INTEGER"),
        bigquery.SchemaField("click", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("rank", "FLOAT"),
    ]


def kakao_moment_ad_schema():
    """카카오 모먼트 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("groupID", "STRING"),
        bigquery.SchemaField("groupName", "STRING"),
        bigquery.SchemaField("creativeID", "STRING"),
        bigquery.SchemaField("creativeName", "STRING"),
        bigquery.SchemaField("imp", "INTEGER"),
        bigquery.SchemaField("click", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
    ]


def google_ads_schema():
    """구글 광고 BigQuery 테이블 스키마. 동적 생성"""
    FIELD_TYPE_MAP = {
        # ID 필드들
        "customer_id": "STRING",
        "campaign_id": "STRING",
        "ad_group_id": "STRING",
        # 이름 필드들
        "customer_descriptive_name": "STRING",
        "campaign_name": "STRING",
        "ad_group_name": "STRING",
        "ad_group_criterion_keyword_text": "STRING",
        # 날짜 필드들
        "segments_date": "DATE",
        "segments_month": "STRING",
        "segments_quarter": "STRING",
        # 상태 필드들
        "campaign_status": "STRING",
        "ad_group_status": "STRING",
        "ad_group_criterion_status": "STRING",
        # 지표 필드들
        "metrics_impressions": "INTEGER",
        "metrics_clicks": "INTEGER",
        "metrics_cost_micros": "FLOAT",
    }
    return FIELD_TYPE_MAP


def ga4_schema():
    """GA4 BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "source": "STRING",
        "medium": "STRING",
        "campaign": "STRING",
        "sessionManualAdContent": "STRING",
        "sessions": "INTEGER",
        "users": "INTEGER",
        "page_views": "INTEGER",
        "bounce_rate": "FLOAT",
        "avg_session_duration": "FLOAT",
        "conversions": "INTEGER",
        "conversion_rate": "FLOAT",
        "eventCount": "INTEGER",
        "keyEvents":"FLOAT",
        "eventValue": "FLOAT",
        "activUsers": "INTEGER",
    }
    return FIELD_TYPE_MAP

def meta_schema():
    """META BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaign_name": "STRING",
        "adset_name": "STRING",
        "ad_name": "STRING",
        "impressions": "INTEGER",
        "clicks": "INTEGER",
        "spend": "FLOAT",
        "video_views": "INTEGER",
    }
    return FIELD_TYPE_MAP


def tiktok_schema():
    """TIKTOK BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATETIME",
        "campaign_id": "STRING",
        "campaign_name": "STRING",
        "adgroup_id": "STRING",
        "adgroup_name": "STRING",
        "ad_id": "STRING",
        "ad_name": "STRING",
        "impressions": "INTEGER",
        "clicks": "INTEGER",
        "spend": "INTEGER",
        "video_play_actions": "INTEGER",
    }
    
    return FIELD_TYPE_MAP

def criteo_schema():
    """CRITEO BigQuery 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "date": "DATE",
        "campaign": "STRING",
        "campaignId": "FLOAT",
        "groupName": "STRING",
        "groupId": "FLOAT",
        "adName": "STRING",
        "imp": "FLOAT",
        "click": "FLOAT",
        "cost": "FLOAT",
    } 
    return FIELD_TYPE_MAP


def naver_campaign_master_schema():
    """네이버 캠페인 마스터 BigQuery 테이블 스키마"""
    return {
        "customerID": "STRING",
        "campaignID": "STRING",
        "campaignName": "STRING",
        "campaignType": "STRING",
        "deliveryMethod": "STRING",
        "usingPeriod": "STRING",
        "periodStartDate": "STRING",
        "periodEndDate": "STRING",
        "regTm": "STRING",
        "delTm": "STRING",
        "on_off": "STRING",
        "sharedBudgetID": "STRING",
    }


def naver_adgroup_master_schema():
    """네이버 광고그룹 마스터 BigQuery 테이블 스키마"""
    return {
        "customerID": "STRING",
        "adGroupID": "STRING",
        "campaignID": "STRING",
        "adGroupName": "STRING",
        "adGroupBidamount": "INTEGER",
        "on_off": "STRING",
        "usingcontentsnetworkbid": "STRING",
        "contentsnetworkbid": "INTEGER",
        "pcNetworkBiddingweight": "INTEGER",
        "mobileNetworkBiddingweight": "INTEGER",
        "businessChannelId_Mobile": "STRING",
        "businessChannelId_PC": "STRING",
        "regTm": "STRING",
        "delTm": "STRING",
        "contentType": "STRING",
        "adGroupType": "STRING",
        "sharedBudgetID": "STRING",
        "usingExpandedSearch": "STRING",
    }


def naver_keyword_master_schema():
    """네이버 키워드 마스터 BigQuery 테이블 스키마"""
    return {
        "customerID": "STRING",
        "adGroupID": "STRING",
        "adKeywordID": "STRING",
        "adKeyword": "STRING",
        "adKeywordBidAmount": "INTEGER",
        "landingURL_PC": "STRING",
        "landingURL_Mobile": "STRING",
        "on_off": "STRING",
        "adKeywordInspectStatus": "STRING",
        "usingAdGroupBidAmount": "STRING",
        "regTm": "STRING",
        "delTm": "STRING",
        "adKeywordType": "STRING",
    }


def naver_shopping_product_master_schema():
    """네이버 쇼핑상품 마스터 BigQuery 테이블 스키마"""
    return {
        "customerID": "STRING",
        "adGroupID": "STRING",
        "adID": "STRING",
        "adCreativeInspectStatus": "STRING",
        "on_off": "STRING",
        "adProductName": "STRING",
        "adImageUrl": "STRING",
        "bid": "INTEGER",
        "usingAdgroupBidAmount": "STRING",
        "adLinkStatus": "STRING",
        "regTm": "STRING",
        "delTm": "STRING",
        "productID": "STRING",
        "productIDofMall": "STRING",
        "productName": "STRING",
        "productImageUrl": "STRING",
        "pcLandingUrl": "STRING",
        "mobileLandingUrl": "STRING",
        "price": "INTEGER",
        "deliveryFee": "INTEGER",
        "naverShoppingCategory1": "STRING",
        "naverShoppingCategory2": "STRING",
        "naverShoppingCategory3": "STRING",
        "naverShoppingCategory4": "STRING",
        "naverShoppingCategoryID1": "STRING",
        "naverShoppingCategoryID2": "STRING",
        "naverShoppingCategoryID3": "STRING",
        "naverShoppingCategoryID4": "STRING",
        "categoryNameofMall": "STRING",
    }


def imweb_inner_data_schema():
    """IMWEB INNER_data BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("site_owner_member", "STRING"),
        bigquery.SchemaField("site_code", "STRING"),
        bigquery.SchemaField("site_creator_member", "STRING"),
        bigquery.SchemaField("main_domain", "STRING"),
        bigquery.SchemaField("site_owner_join_time", "TIMESTAMP"),
        bigquery.SchemaField("site_creation_time", "TIMESTAMP"),
        bigquery.SchemaField("first_plan_payment_time", "TIMESTAMP"),
        bigquery.SchemaField("first_plan_version", "STRING"),
        bigquery.SchemaField("first_plan_period", "FLOAT"),
        bigquery.SchemaField("first_plan_price_with_tax", "FLOAT"),
        bigquery.SchemaField("plan_end_time", "TIMESTAMP"),
        bigquery.SchemaField("trial_start_time", "TIMESTAMP"),
        bigquery.SchemaField("trial_version", "STRING"),
        bigquery.SchemaField("is_subscription_active", "BOOLEAN"),
        bigquery.SchemaField("subscription_type", "STRING"),
        bigquery.SchemaField("is_pg_or_pay_active", "BOOLEAN"),
        bigquery.SchemaField("site_payment_lead_time", "FLOAT"),
        bigquery.SchemaField("first_payment_operation_type", "STRING"),
    ]

def hanssem_insight_schema():
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("media", "STRING"),
        bigquery.SchemaField("utm_campaign", "STRING"),
        bigquery.SchemaField("utm_content", "STRING"),
        bigquery.SchemaField("utm_content_1", "STRING"),
        bigquery.SchemaField("utm_content_2", "STRING"),
        bigquery.SchemaField("utm_content_3", "STRING"),
        bigquery.SchemaField("utm_content_4", "STRING"),
        bigquery.SchemaField("utm_content_5", "STRING"),
        bigquery.SchemaField("utm_content_6", "STRING"),
        bigquery.SchemaField("utm_content_7", "STRING"),
        bigquery.SchemaField("utm_content_8", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("consultation", "INTEGER"),
        bigquery.SchemaField("distribution", "INTEGER"),
    ]

def dmp_schema():
    """IMWEB FIRST PARTY DATA 테이블 스키마"""
    FIELD_TYPE_MAP = {
        "event": "STRING",
        "imwebUserid": "STRING",
        "email": "STRING",
        "phone_number": "STRING",
        "event_id": "STRING"
    } 
    return FIELD_TYPE_MAP