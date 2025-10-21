from clients.gfa_api_client import GFAAPIClient
from typing import List, Dict, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class GFAReportService:
    def __init__(self, gfa_client: GFAAPIClient):
        self.client = gfa_client

    async def get_performance_data(self):
        report = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        response = await self.client.get_performance(yesterday, yesterday)
        performance_data: List[Dict[str, Any]] = response.get("rows", [])

        index = {"campaigns": {}, "adsets": {}, "creatives": {}}

        for target in index:
            logger.info(target)
            structure_info = await self.get_ad_structure_list(target)
            index[target] = structure_info
        
        for data in performance_data:
            data["campaign_name"] = index["campaigns"][data["campaignNo"]]
            data["adset_name"] = index["adsets"][data["adSetNo"]]
            data["creative_name"] = index["creatives"][data["creativeNo"]]
            report.append(data)
        
        return report

    async def get_ad_structure_list(self, target):
        """ 캠페인, 광고그룹, 소재 목록 반환"""
        structure_list = []
        api_list = {"campaigns": self.client.get_campaigns, 
                    "adsets": self.client.get_adsets,
                    "creatives": self.client.get_creatives}

        response = await api_list[target]()
        
        structure_list.extend(response.get("content", []))
        total_page = int(response.get("totalPages", 1))
        
        for page in range(1, total_page):
            response = await api_list[target](page)
            structure_list.extend(response.get("content", []))

        structure_info = {}
        for structure in structure_list:
            structure_id, structure_name = structure["no"], structure["name"]
            structure_info[structure_id] = structure_name
        
        return structure_info
    