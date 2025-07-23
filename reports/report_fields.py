
def naver_campaign_fields():
    campaign_headers = ["CustomerID", "CampaignID", "CampaignName", "Campaign Type", 
                    "Delivery Method", "UsingPeriod", "PeriodStartDate", "PeriodEndDate",
                    "regTm", "delTm", "ON/OFF", "SharedBudgetID"]

    return campaign_headers

def naver_ad_group_fields():
    ad_group_headers = ["CustomerID", "AdGroupID", "CampaignID", "AdGroupName", "AdGroupBidamount",
                        "ON/OFF", "	Usingcontentsnetworkbid", "Contentsnetworkbid",
                        "PCnetworkbiddingweight", "Mobilenetworkbiddingweight",
                        "BusinessChannelId(Mobile)", "BusinessChannelId(PC)", "regTm", "delTm", "ContentType",
                        "AdGroupType", "SharedBudgetID", "UsingExpandedSearch"]
    
    return ad_group_headers

def naver_keyword_fields():
    keyword_headers = ["CustomerID", "AdGroupID", "AdKeywordID", "AdKeyword", "AdKeywordBidAmount",
                        "landingURL(PC)", "landingURL(Mobile)", "ON/OFF", "AdKeywordInspectStatus",
                        "UsingAdGroupBidAmount", "regTm", "delTm", "AdKeywordType"]
    
    return keyword_headers

def naver_ad_fields():
    ad_report_headers = ["Date", "CUSTOMERID", "CampaignID", "AdGroupID", "AdKeywordID",
                        "ADID", "BusinessChannelID", "Mediacode", "PCMobileType",
                        "Impressions", "Clicks", "Cost", "SumofADrank", "Viewcount"]

    return ad_report_headers

def naver_vaild_fields():
    vaild_headers = ["Date", "CampaignID", "CampaignName", "AdGroupID", "AdGroupName",
                     "AdKeywordID", "AdKeyword", "Impressions", "Clicks", "Cost", "SumofADrank"]
    
    return vaild_headers