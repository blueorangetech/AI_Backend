customers = {
    "speed_mate" : {
        "table_name": "speed_mate",
        "media_list": {
            "naver" : {
                "customer_id": "2373367",
                "stat_type": ["AD", "AD_CONVERSION"]
                },
            "google_ads" :{
                "customer_id": "",
                "fields":[
                    "segments.device", "ad_group_ad.ad.type", "campaign.name",
                    "ad_group.name", "metrics.impressions", "metrics.clicks",
                    "metrics.cost_micros"
                    ]
                },
            "ga4": {
                "porperty_id": "",
                "fields":[
                    {
                        "default":[
                            "date", "sessionSourceMedium", "deviceCategory", 
                            "sessionCampaignName", "sessionManualAdContent",
                            ],
                        "custom": [
                                "maintenance_normal_reserve_cplt", 
                                    "maintenance_imported_reserve_cplt"
                                    ]
                                },
                                {
                                "default": [
                                    "date", "deviceCategory", "sessionSourceMedium",
                                    "sessionCampaignName", "sessionManualAdContent",
                                    "sessions"
                                ],
                                "custom": []
                                }
                            ]
                        }
        }
    }
}