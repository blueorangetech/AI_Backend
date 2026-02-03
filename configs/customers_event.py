bo_customers = {
    "imweb":{
        "data_set_name": "imweb",
        "dmp_table": "IMWEB_dmp",
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
                        "view_level": "FROM ad_group_ad",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0"
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
                        "view_level": "FROM campaign",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0"
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
                        "view_level": "FROM keyword_view",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0"
                    }
                }
            },
            "ga4": {
                "property_id": "310647872",
                "fields": {
                    "total": {
                        "default": [
                            "date", "sessionCampaignName", 
                            "sessionMedium", "sessionSource", "sessionManualAdContent", 
                            "customEvent:site_url", "customUser:user_id_dimension", 
                            "customEvent:site_code", "eventName"
                                ],
                            "metric":["eventCount"],
                            "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                        },
                    "total_users": {
                        "default": [
                            "date", "campaignName", "medium", "source", "ManualAdContent",
                            "customEvent:site_url", "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                                ],
                            "metric":["keyEvents"],
                            "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                            "date_range": "30daysAgo"
                        },
                    "keyword":{
                        "default": [
                            "date", "sessionCampaignName",
                            "sessionManualTerm", "customEvent:site_url",
                            "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                                ],
                        "metric": ["eventCount"],
                        "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                        },
                        
                    "keyword_users":{
                        "default": [
                            "date", "campaignName",
                            "manualTerm", "customEvent:site_url",
                            "customUser:user_id_dimension", "customEvent:site_code", "eventName"
                                ],
                        "metric": ["keyEvents"],
                        "filter":["join", "sign_up", "join_complete_landing", "FreeTrial", "purchase"],
                        "date_range": "30daysAgo"
                        },

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
    "atria": {
        "data_set_name": "atria",
        "media_list": {
            "naver": {"customer_id": "1099869",
                      "master_list": ["Campaign", "Adgroup", "Keyword", "ShoppingProduct"],
                      "stat_types": ["AD", "AD_CONVERSION", "SHOPPINGKEYWORD_DETAIL", "SHOPPINGKEYWORD_CONVERSION_DETAIL"]
                      },
        }
    },
    "hanssem_hf": {
        "data_set_name": "hanssem_hf",
        "media_list": {
            "google_ads": {
                "customer_id": "3192418383",
                "fields": {
                    "convert_map": {
                        "fields": [
                            "conversion_action.resource_name", 
                            "conversion_action.name"
                        ],
                        "view_level": "FROM conversion_action"
                    },
                    "asset_convert": {
                        "fields": [
                            "segments.date",
                            "asset.id",
                            "asset.name",
                            "asset.type",
                            "asset.image_asset.full_size.url",
                            "asset.youtube_video_asset.youtube_video_id",
                            "ad_group_ad_asset_view.field_type",
                            "ad_group_ad_asset_view.performance_label",
                            "segments.conversion_action",
                            "metrics.conversions",
                            "metrics.all_conversions",
                            "campaign.name",
                            "ad_group.name"
                        ],
                        "view_level": "FROM ad_group_ad_asset_view",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.conversions > 0 AND campaign.advertising_channel_type = 'MULTI_CHANNEL'"
                    },
                    "asset": {
                        "fields": [
                            "segments.date",
                            "asset.id",
                            "asset.name",
                            "asset.type",
                            "ad_group_ad_asset_view.field_type",
                            "asset.image_asset.full_size.width_pixels",
                            "asset.image_asset.full_size.height_pixels",
                            "ad_group_ad_asset_view.performance_label",
                            "metrics.impressions",
                            "metrics.clicks",
                            "metrics.cost_micros",
                            "campaign.name",
                            "ad_group.name"
                        ],
                        "view_level": "FROM ad_group_ad_asset_view",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0 AND campaign.advertising_channel_type = 'MULTI_CHANNEL'"
                    }
                }
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
                        "view_level": "FROM ad_group_ad",
                        "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0"
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
                "view_level": "FROM ad_group_ad",
                "conditions": "WHERE segments.date DURING YESTERDAY AND metrics.impressions > 0"
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
