from google.cloud import bigquery

def get_naver_search_ad_schema():
    """네이버 검색광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("CampaignName", "STRING"),
        bigquery.SchemaField("CampaignID", "STRING"),
        bigquery.SchemaField("AdGroupName", "STRING"),
        bigquery.SchemaField("AdGroupID", "STRING"),
        bigquery.SchemaField("AdKeyword", "STRING"),
        bigquery.SchemaField("AdKeywordID", "STRING"),
        bigquery.SchemaField("Impressions", "INTEGER"),
        bigquery.SchemaField("Clicks", "INTEGER"),
        bigquery.SchemaField("Cost", "FLOAT"),
        bigquery.SchemaField("SumofADrank", "FLOAT"),
    ]

def get_kakao_ad_schema():
    """카카오 광고 BigQuery 테이블 스키마"""
    return [
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("adgroup_name", "STRING"),
        bigquery.SchemaField("adgroup_id", "STRING"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("cpc", "FLOAT"),
        bigquery.SchemaField("conversions", "INTEGER"),
        bigquery.SchemaField("conversion_rate", "FLOAT"),
        bigquery.SchemaField("cost_per_conversion", "FLOAT"),
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