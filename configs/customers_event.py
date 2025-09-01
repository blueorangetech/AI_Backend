bo_customers = {
    "speed_mate" : {
        "data_set_name": "speed_mate",
        "media_list": {
            "naver" : {
                "customer_id": "2373367",
                "stat_types": ["AD", "AD_CONVERSION"]
                },
            "google_ads" :{
                "customer_id": "8272761282",
                "fields":[
                    "segments.date", "segments.device", 
                    "ad_group_ad.ad.type", "campaign.name",
                    "ad_group.name", "metrics.impressions", "metrics.clicks",
                    "metrics.cost_micros"
                    ],
                "view_level": "From ad_group_ad"
                },
            "ga4": {
                "property_id": "376866221",
                "fields":{
                    "conversion": {
                        "default":[
                            "date", "sessionSourceMedium", "deviceCategory", 
                            "sessionCampaignName", "sessionManualAdContent"
                            ],
                        "custom": [
                            "maintenance_normal_reserve_cplt",
                            "maintenance_imported_reserve_cplt",
                            "purchase"
                            ],
                        "metric": ["activeUsers"]
                        },
                    "inflow": {
                        "default": [
                            "date", "deviceCategory", "sessionSourceMedium",
                            "sessionCampaignName", "sessionManualAdContent"
                            ],
                        "custom": [],
                        "metric": ["sessions", "activeUsers"]
                        }
                }
            }
        }
    }
}