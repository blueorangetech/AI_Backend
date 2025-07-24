from google.cloud import bigquery

def get_naver_search_ad_schema():
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

def get_kakao_search_ad_schema():
    """카카오 검색광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "Date"),
        bigquery.SchemaField("campaignID", "STRING"),
        bigquery.SchemaField("campaignName", "STRING"),
        bigquery.SchemaField("groupID", "STRING"),
        bigquery.SchemaField("groupName", "STRING"),
        bigquery.SchemaField("keywordID", "STRING"),
        bigquery.SchemaField("keywordName", "STRING"),
        bigquery.SchemaField("imp", "INTEGER"),
        bigquery.SchemaField("click", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("rank", "FLOAT")
    ]

def get_google_ads_schema():
    """구글 광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("ad_group_name", "STRING"),
        bigquery.SchemaField("ad_group_id", "STRING"),
        bigquery.SchemaField("keyword_text", "STRING"),
        bigquery.SchemaField("keyword_id", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cost_micros", "INTEGER"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("average_cpc", "FLOAT"),
        bigquery.SchemaField("conversions", "FLOAT"),
        bigquery.SchemaField("conversion_rate", "FLOAT"),
        bigquery.SchemaField("cost_per_conversion", "FLOAT"),
    ]

def get_ga4_schema():
    """GA4 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("medium", "STRING"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("sessions", "INTEGER"),
        bigquery.SchemaField("users", "INTEGER"),
        bigquery.SchemaField("page_views", "INTEGER"),
        bigquery.SchemaField("bounce_rate", "FLOAT"),
        bigquery.SchemaField("avg_session_duration", "FLOAT"),
        bigquery.SchemaField("conversions", "INTEGER"),
        bigquery.SchemaField("conversion_rate", "FLOAT"),
    ]