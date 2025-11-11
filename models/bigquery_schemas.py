from google.cloud import bigquery


def naver_search_ad_schema():
    """네이버 검색광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("adGroupName", "STRING"),
        bigquery.SchemaField("adGroupID", "STRING"),
        bigquery.SchemaField("adKeyword", "STRING"),
        bigquery.SchemaField("adKeywordID", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("sumofADrank", "FLOAT"),
    ]


def naver_search_ad_cov_schema():
    """네이버 검색광고 전환 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("adGroupName", "STRING"),
        bigquery.SchemaField("adGroupID", "STRING"),
        bigquery.SchemaField("adKeyword", "STRING"),
        bigquery.SchemaField("adKeywordID", "STRING"),
        bigquery.SchemaField("conversionType", "STRING"),
        bigquery.SchemaField("conversionCount", "FLOAT"),
        bigquery.SchemaField("salesByConversion", "INTEGER"),
    ]

def naver_shopping_ad_schema():
    """네이버 쇼핑광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("adGroupID", "STRING"),
        bigquery.SchemaField("adGroupName", "STRING"),
        bigquery.SchemaField("searchKeyword", "STRING"),
        bigquery.SchemaField("adID", "STRING"),
        bigquery.SchemaField("productName", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("sumofADrank", "FLOAT"),
    ]

def naver_shopping_ad_cov_schema():
    """네이버 쇼핑광고 전환 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("adGroupID", "STRING"),
        bigquery.SchemaField("adGroupName", "STRING"),
        bigquery.SchemaField("searchKeyword", "STRING"),
        bigquery.SchemaField("adID", "STRING"),
        bigquery.SchemaField("productName", "STRING"),
        bigquery.SchemaField("conversionType", "STRING"),
        bigquery.SchemaField("conversionCount", "FLOAT"),
        bigquery.SchemaField("salesByConversion", "INTEGER"),
    ]

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
        "sessionManualAdContent": "STRING",
        "sessionManualAdContent": "STRING",
        "campaign": "STRING",
        "sessions": "INTEGER",
        "users": "INTEGER",
        "page_views": "INTEGER",
        "bounce_rate": "FLOAT",
        "avg_session_duration": "FLOAT",
        "conversions": "INTEGER",
        "conversion_rate": "FLOAT",
        "eventCount": "INTEGER",
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
    }
    return FIELD_TYPE_MAP


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
