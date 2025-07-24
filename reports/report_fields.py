
def naver_campaign_fields():
    campaign_headers = ["customerID", "campaignID", "campaignName", "campaign Type", 
                    "delivery Method", "usingPeriod", "periodStartDate", "periodEndDate",
                    "regTm", "delTm", "on/off", "sharedBudgetID"]

    return campaign_headers

def naver_ad_group_fields():
    ad_group_headers = ["customerID", "adGroupID", "campaignID", "adGroupName", "adGroupBidamount",
                        "oN/OFF", "	usingcontentsnetworkbid", "contentsnetworkbid",
                        "pCnetworkbiddingweight", "mobilenetworkbiddingweight",
                        "businessChannelId(Mobile)", "businessChannelId(PC)", "regTm", "delTm", "contentType",
                        "adGroupType", "sharedBudgetID", "usingExpandedSearch"]
    
    return ad_group_headers

def naver_keyword_fields():
    keyword_headers = ["customerID", "adGroupID", "adKeywordID", "adKeyword", "adKeywordBidAmount",
                        "landingURL(PC)", "landingURL(Mobile)", "oN/OFF", "adKeywordInspectStatus",
                        "usingAdGroupBidAmount", "regTm", "delTm", "adKeywordType"]
    
    return keyword_headers

def naver_ad_fields():
    ad_report_headers = ["date", "customerID", "campaignID", "adGroupID", "adKeywordID",
                        "aDID", "businessChannelID", "mediacode", "pCMobileType",
                        "impressions", "clicks", "cost", "sumofADrank", "viewcount"]

    return ad_report_headers

def naver_vaild_fields():
    vaild_headers = ["date", "campaignID", "campaignName", "adGroupID", "adGroupName",
                     "adKeywordID", "adKeyword", "impressions", "clicks", "cost", "sumofADrank"]
    
    return vaild_headers