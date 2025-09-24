bo_customers = {
    "speed_mate": {
        "data_set_name": "speed_mate",
        "media_list": {
            "naver": {"customer_id": "2373367", 
                      "master_list": ["Campaign", "Adgroup", "Keyword"],
                      "stat_types": ["AD", "AD_CONVERSION"]},
            "google_ads": {
                "customer_id": "8272761282",
                "fields": [
                    "segments.date",
                    "segments.device",
                    "ad_group_ad.ad.type",
                    "campaign.name",
                    "ad_group.name",
                    "metrics.impressions",
                    "metrics.clicks",
                    "metrics.cost_micros",
                ],
                "view_level": "From ad_group_ad",
            },
            "ga4": {
                "property_id": "376866221",
                "fields": {
                    "conversion": {
                        "default": [
                            "date",
                            "sessionSourceMedium",
                            "deviceCategory",
                            "sessionCampaignName",
                            "sessionManualAdContent",
                        ],
                        "custom": [
                            "maintenance_normal_reserve_cplt",
                            "maintenance_imported_reserve_cplt",
                            "purchase",
                        ],
                        "metric": ["activeUsers"],
                    },
                    "inflow": {
                        "default": [
                            "date",
                            "deviceCategory",
                            "sessionSourceMedium",
                            "sessionCampaignName",
                            "sessionManualAdContent",
                        ],
                        "custom": ["session_start"],
                        "metric": ["activeUsers"],
                    },
                },
            },
        },
    },
    "lgcare": {
        "data_set_name": "lgcare",
        "media_list": {
            "naver": {"customer_id": "2033063", 
                      "master_list" : ["Campaign", "Adgroup", "Keyword", "ShoppingProduct"],
                      "stat_types": ["AD", "AD_CONVERSION", "SHOPPINGKEYWORD_DETAIL", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
                      },
        },
    },
    "htb": {
        "data_set_name": "htb",
        "media_list": {
            "naver": {"customer_id": "2881738",
                      "master_list" : ["Campaign", "Adgroup", "ShoppingProduct"],
                      "stat_types": ["SHOPPINGKEYWORD_DETAIL", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
                      }
        },
    },
    "baemin_restaurant":{
        "data_set_name": "baemin_restaurant",
        "media_list": {
            # "naver": {"customer_id": "1821082",
            #           "master_list" : ["Campaign", "Adgroup", "Keyword"],
            #           "stat_types": ["AD"],
            #           },
            # "kakao_moment":{
            #     "account_id": "620017"
            # },
            "google_ads": {
                "customer_id": "7107846694",
                "fields": [
                    "segments.date",
                    "campaign.name",
                    "ad_group.name",
                    "ad_group_ad.ad.final_urls",
                    "metrics.impressions",
                    "metrics.video_views",
                    "metrics.clicks",
                    "metrics.cost_micros",
                    "metrics.conversions",
                ],
                "view_level": "From ad_group_ad",
            }
        }
    }
}
