from clients.kakao_api_client import KakaoAPIClient
import os, jwt, asyncio
import pandas as pd

class KakaoReportService:
    def __init__(self, token, account_id):
        self.client = KakaoAPIClient(token, account_id)
    
    async def create_report(self, start_date, end_date):
        index_data = await self._create_report_index()
        report_data = await self._load_report(start_date, end_date)
        result = await self._merge_index(index_data, report_data)
        return 
        
    async def _create_report_index(self):
        campaigns = await self.client.get_campaigns_info()
        groups, keywords = {}, {}

        for campagin in campaigns:
            campagin_group = await self.client.get_groups_info(campagin)
            groups.update(campagin_group)


        for group in groups:
            group_keyword = await self.client.get_keywords_info(group)
            keywords.update(group_keyword)

        index_data = {"campaigns": campaigns, "groups": groups, "keywords": keywords}
        return index_data
    

    async def _load_report(self, start_date, end_date):
        campaigns = await self.client.get_campaigns_info()
        report_data = pd.DataFrame(columns=["date", "campaign", "group", "keyword", "imp", "click", "cost"])


        for campaign in campaigns:
            reports = await self.client.get_report(campaign, start_date, end_date)
            campaign_result = await self._concat_data(reports)
            report_data = pd.concat([report_data, campaign_result], ignore_index=True)

            await asyncio.sleep(1)
        
        return report_data
        
    async def _concat_data(self, reports):
        result = {}

        for index, report in enumerate(reports["data"]):
            date = report["start"]
            
            campaign = report["dimensions"]["campaignId"]
            group = report["dimensions"]["adGroupId"]
            keyword = report["dimensions"]["keywordId"]

            imp = report["metrics"]["imp"]
            click = report["metrics"]["click"]
            cost = report["metrics"]["spending"]

            result[index] = {"date": date, "campaign": campaign, "group": group,
                             "keyword": keyword, "imp": imp, "click": click, "cost": cost}
        
        campaign_result = pd.DataFrame.from_dict(result, orient="index")
        return campaign_result
    
    async def _merge_index(self, index_data, report_data):
        report_data["campagin_name"] = report_data["campaign"].apply(lambda x: index_data["campaigns"][x])
        report_data["group_name"] = report_data["group"].apply(lambda x: index_data["groups"][x])
        report_data["keyword_name"] = report_data["keyword"].apply(lambda x: index_data["keywords"][x])

        return report_data