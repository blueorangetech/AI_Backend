bo_customers = {
    "imweb":{
        "data_set_name": "imweb",
        "media_list": {
            "naver": {
                "customer_id": "1079588",
                "master_list": ["Campaign", "Adgroup", "Keyword"],
                "stat_types": ["AD"]
            },
            "kakao_moment": { 
                "account_id": "296766"
            },
            "gfa": {
                "customer_id": "17427"
            },
            "meta": {
                "account_id": "264857540",
                "fields": [
                    "date_start",
                    "campaign_name",
                    "adset_name",
                    "ad_name",
                    "impressions",
                    "clicks",
                    "spend",
                    "video_play_actions"
                ],
            },
            "google_ads": {
                "customer_id": "9984343390",
                "fields": {
                    "total": {
                        "fields" :[
                            "segments.date",
                            "campaign.name",
                            "ad_group.name",
                            "ad_group_ad.ad.name",
                            "metrics.impressions",
                            "metrics.clicks",
                            "metrics.cost_micros",
                            "metrics.video_views",
                            ],
                        "view_level": "From ad_group_ad",
                    },
                    "campaign":{
                        "fields": [
                            "segments.date",
                            "campaign.name",
                            "metrics.impressions",
                            "metrics.clicks",
                            "metrics.cost_micros",
                            "metrics.video_views",
                        ],
                        "view_level": "From campaign",
                    },
                    "keyword": {
                        "fields": [
                            "segments.date",
                            "campaign.name",
                            "ad_group_criterion.keyword.text",
                            "metrics.impressions",
                            "metrics.clicks",
                            "metrics.cost_micros",

                        ],
                        "view_level": "From keyword_view",
                    }
                }
            },
            "ga4": {
                "property_id": "310647872",
                "fields": {
                    "total": {
                        "default": [
                            "date", "sessionCampaignName", 
                            "sessionMedium", "sessionSource", "customEvent:site_url",
                            "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                                ],
                            "metric":["eventCount"],
                            "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                        },
                    # "test": {
                    #     "default": [
                    #         "date", "campaignName", 
                    #         "medium", "source", "customEvent:site_url",
                    #         "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                    #             ],
                    #         "custom":[],
                    #         "metric":["eventCount"],
                    #     },
                    "keyword":{
                        "default": [
                            "date", "sessionCampaignName",
                            "sessionManualTerm", "customEvent:site_url",
                            "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                                ],
                        "metric": ["eventCount"],
                        "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                        }
                    }
            },
            "tiktok":{
                "account_id": "7311146119936000001",
                "dimensions": ["stat_time_day", "ad_id"],
                "metrics": [ "campaign_id", "campaign_name","adgroup_id", "adgroup_name", "ad_name", 
                            "impressions", "clicks", "spend", "video_play_actions"
                            ],
            },
            "criteo": {
                "mailfolder": "121",
                "report_name": "imweb_report",
                "field_names": ["date", "campaign", "campaignId", "groupName", "groupId", 
                                "adName", "imp", "click", "cost"]
            }
        }
    },
    "speed_mate": {
        "data_set_name": "speed_mate",
        "media_list": {
            "naver": {"customer_id": "2373367", 
                      "master_list": ["Campaign", "Adgroup", "Keyword"],
                      "stat_types": ["AD", "AD_CONVERSION"]},
            "google_ads": {
                "customer_id": "8272761282",
                "fields":{
                    "total": {
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
                    }
                },
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
                            "eventName"
                        ],
                        "metric": ["activeUsers"],
                        "filter": [
                            "maintenance_normal_reserve_cplt",
                            "maintenance_imported_reserve_cplt",
                            "purchase",
                        ],
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
            "naver": {"customer_id": "1821082",
                      "master_list" : ["Campaign", "Adgroup", "Keyword"],
                      "stat_types": ["AD"],
                      },
            "kakao_moment":{
                "account_id": "620017"
            },
            "gfa": {
                "customer_id": "66953"
            },
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
            },
            "meta": {
                "account_id":"1223120648245224",
                "fields": [
                    "date_start",
                    "campaign_name",
                    "adset_name",
                    "ad_name",
                    "impressions",
                    "clicks",
                    "spend",
                    ]
                }
            }
        }
    }
