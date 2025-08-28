from clients.kakao_api_client import KakaoAPIClient
import pandas as pd
import asyncio, logging, time

logger = logging.getLogger(__name__)


class KakaoReportService:
    def __init__(self, token, account_id):
        self.client = KakaoAPIClient(token, account_id)
    
    # Kakao Keyword Report
    async def create_report(self):
        campaigns = await self.client.get_campaigns_info()

        index_data = await self._create_report_index(campaigns)
        report_data = await self._load_report(campaigns)
        
        merge_data = await self._merge_index(index_data, report_data)
        
        merge_data["date"] = pd.to_datetime(merge_data["date"]).dt.strftime('%Y-%m-%d')
        keyword_report = merge_data.to_dict('records')

        result = {"kakao_keyword": keyword_report}
        return result
        
    async def _create_report_index(self, campaigns):
        groups, keywords = {}, {}

        for campaign in campaigns:
            campaign_group = await self.client.get_groups_info(campaign)
            groups.update(campaign_group)

        for group in groups:
            group_keyword = await self.client.get_keywords_info(group)
            keywords.update(group_keyword)

        index_data = {"campaigns": campaigns, "groups": groups, "keywords": keywords}
        return index_data
    
    async def _load_report(self, campaigns):        
        report_data = pd.DataFrame(columns=["date", "campaignID", "groupID", "keywordID", "imp", "click", "cost", "rank"])

        for campaign in campaigns:
            reports = await self.client.get_report(campaign)
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
            rank = report["metrics"]["rank"]

            result[index] = {"date": date, "campaignID": campaign, "groupID": group,
                             "keywordID": keyword, "imp": imp, "click": click, "cost": cost, "rank": rank}
        
        campaign_result = pd.DataFrame.from_dict(result, orient="index")
        return campaign_result
    
    async def _merge_index(self, index_data, report_data):
        report_data["campaignName"] = report_data["campaignID"].map(index_data["campaigns"])
        report_data["groupName"] = report_data["groupID"].map(index_data["groups"])
        report_data["keywordName"] = report_data["keywordID"].map(index_data["keywords"])

        return report_data
    
    # Kakao Moment Report
    async def create_moment_report(self):
        index_data = await self._create_moment_index()
        creatives_list = list(index_data["creatives"]["name"].keys())
        creatives_report = await self._create_moment_report(creatives_list)
        report = await self._filter_concat_data(creatives_report)

        report["creativeName"] = report["creativeID"].map(index_data["creatives"]["name"])
        report["groupID"] = report["creativeID"].map(index_data["creatives"]["group"])
        report["groupName"] = report["groupID"].map(index_data["groups"]["name"])
        report["campaignID"] = report["groupID"].map(index_data["groups"]["campaign"])
        report["campaignName"] = report["campaignID"].map(index_data["campaigns"])

        kakao_moment = report.to_dict('records')
        result = {"kakao_moment": kakao_moment}
        return result
    
    async def _create_moment_index(self):
        campagins = await self.client.get_moment_campaigns_info()
        groups = {"name": {}, "campaign": {}}
        creatives = {"name": {}, "group": {}}

        for campaign in campagins:
            campaign_group = await self.client.get_moment_groups_info(campaign)
            groups["name"].update(campaign_group["name"])
            groups["campaign"].update(campaign_group["campaign"])
        
        for group in groups["name"]:
            group_creative = await self.client.get_moment_creatives_info(group)
            creatives["name"].update(group_creative["name"])
            creatives["group"].update(group_creative["group"])
        
        index_data = {"campaigns": campagins, "groups": groups, "creatives": creatives}
        return index_data
    
    async def _create_moment_report(self, creatives_list):
        creatives_report = []

        # 전체 리포트 데이터 호출
        for i in range(0, len(creatives_list), 100):
            chunk = creatives_list[i: i+100]
            creatives_ids = ','.join(map(str, chunk))
            
            chunk_response = await self.client.get_moment_report(creatives_ids)
            creatives_report += chunk_response

            if i + 100 < len(creatives_list):
                await asyncio.sleep(5.3)

        return creatives_report
    
    async def _filter_concat_data(self, creatives_report):
        result = {}

        # metrices 길이 0 은 제외 후 추출
        for index, report in enumerate(creatives_report):
            if len(report["metrics"]) == 0:
                continue
            
            date = report["start"]
            creative_id = str(report["dimensions"]["creative_id"])
            imp = report["metrics"]["imp"]
            click = report["metrics"]["click"]
            cost = report["metrics"]["cost"]
            
            result[index] = {"date": date, "creativeID": creative_id, "imp": imp, "click": click, "cost": cost}

        report_data = pd.DataFrame.from_dict(result, orient="index")
        return report_data