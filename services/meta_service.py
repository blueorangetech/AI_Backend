from clients.meta_ads_api_client import MetaAdsAPIClient
import logging

logger = logging.getLogger(__name__)

class MetaAdsReportServices:

    def __init__(self, meat_client: MetaAdsAPIClient):
        self.client = meat_client
    
    async def create_reports(self, fields):
        # 권한 확인
        auth_check = await self._check_account_auth()

        # 리포트 생성
        if auth_check:
            items = await self.client.get_adset_performance(fields)
            result = await self._processing_report(items, fields)
            return result
        
        else:
            return "권한을 확인하세요"
        
    
    async def _check_account_auth(self):
        response = await self.client.verify_account_in_list()
        return response
    

    async def _processing_report(self, items, fields):
        result = []
        for item in items:
            row = {}
            for field in fields:
                row[field] = item.get(field)

            result.append(row)
            #     # video_play_actions에서 video_view value 추출
            #     video_views = 0
            #     if 'video_view' in item:
            #         for action in item['video_play_actions']: # type: ignore
            #             if action.get('action_type') == 'video_view':# type: ignore
            #                 video_views = int(action.get('value', 0))# type: ignore
            #                 break
            #     row['video_views'] = video_views
        return result