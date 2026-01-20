from clients.gfa_api_client import GFAAPIClient
from typing import List, Dict, Any
from datetime import datetime, timedelta
import logging, asyncio

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
            structure_list = await self.get_ad_structure_list(target)
            structure_info = {}

            for structure in structure_list:
                structure_id, structure_name = structure["no"], structure["name"]
                structure_info[structure_id] = structure_name
                
            index[target] = structure_info
        
        for data in performance_data:
            data["campaign_name"] = index["campaigns"][data["campaignNo"]]
            data["adset_name"] = index["adsets"][data["adSetNo"]]
            data["creative_name"] = index["creatives"][data["creativeNo"]]
            data["date"] = data.pop("targetDate")
            report.append(data)
        
        result = {"NAVER_GFA": report}
        return result
    
    async def adjust_budget(self, type: str):
        """ 예산을 변경합니다. IMWEB 한정 """
        results = []
        # 캠페인 On인 리스트 생성
        # 그룹 리스트 On 반환 후 캠페인 On 리스트만 선별

        live_data = {"campaigns": [], "adsets": []}

        for target in live_data:
            structure_list = await self.get_ad_structure_list(target, activated=True)
            
            for structure in structure_list:
                if target == "campaigns":
                    data = structure["no"]
                else:
                    data = {"adset_id": structure["no"],
                            "campaign_id": structure["campaignNo"],
                            "amount": structure["budgetAmount"]
                            }

                live_data[target].append(data)
        
        total, success = 0,0
        for adset in live_data["adsets"]:
            if adset["campaign_id"] in live_data["campaigns"]:
                logger.info(adset)
                total += 1
                adjust_amount = adset["amount"] * 2 if type.upper() == "UP" else adset["amount"] / 2
                response = await self.client.adjust_adset_budget(adset["adset_id"], adjust_amount)
                await asyncio.sleep(0.5)
                if response["success"]:
                    success += 1
        
        return f"{success}/{total} 예산 변경 완료"
    
    async def get_ad_structure_list(self, target, activated: Any = None):
        """ 캠페인, 광고그룹, 소재 목록 반환"""
        # On / All 상태인 데이터 식별 하도록 추가
        # Client 코드 변경
        structure_list = []
        api_list = {"campaigns": self.client.get_campaigns, 
                    "adsets": self.client.get_adsets,
                    "creatives": self.client.get_creatives}

        response = await api_list[target]() if activated is None else await api_list[target](activated=activated)
        
        structure_list.extend(response.get("content", []))
        total_page = int(response.get("totalPages", 1))
        
        for page in range(1, total_page):
            await asyncio.sleep(0.5)
            response = await api_list[target](page)
            structure_list.extend(response.get("content", []))
        
        return structure_list
    