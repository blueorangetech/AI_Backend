from clients.gfa_api_client import GFAAPIClient
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class GFAReportService:
    def __init__(self, gfa_client: GFAAPIClient):
        self.client = gfa_client

    async def get_performance_data(self):
        report = []

        response = await self.client.get_performance("2025-10-18", "2025-10-19")
        performance_data: List[Dict[str, Any]] = response.get("rows", [])

        index = {"campagins": {}, "adsets": {}, "creatives": {}}

        for target in index.keys():
            structure_info = await self.get_ad_structure_list(target)
            index[target] = structure_info

        
        for data in performance_data:
            data["campaign_name"] = index["campagins"][data.get("campaginNo")]
            data["adset_name"] = index["adsets"][data.get("adSetNo")]
            data["creative_name"] = index["creatives"][data.get("creativeNo")]
            return data
            report.append(data)
        logger.info(report)
        return report

    async def get_ad_structure_list(self, target):
        """ 캠페인, 광고그룹, 소재 목록 반환"""
        structure_list = []

        if target == "campaign":
            response = await self.client.get_campaigns()

        elif target == "adset":
            response = await self.client.get_adsets()
        
        else: # creatives
            response = await self.client.get_creatives()
        
        structure_list.extend(response.get("content", []))
        total_page = int(response.get("totalPages", 1))
        
        for page in range(1, total_page):
            response = await self.client.get_campaigns(page)
            structure_list.extend(response.get("content", []))

        structure_info = {}
        for campaign in structure_list:
            structure_id, structure_name = campaign["no"], campaign["name"]
            structure_info[structure_id] = structure_name
        
        return structure_info
    