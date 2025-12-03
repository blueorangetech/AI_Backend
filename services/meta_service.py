from clients.meta_ads_api_client import MetaAdsAPIClient
import logging

logger = logging.getLogger(__name__)


class MetaAdsReportServices:
    def __init__(self, meat_client: MetaAdsAPIClient):
        self.client = meat_client

    async def create_reports(self, fields):
        # 리포트 생성
        try:
            items = await self.client.get_adset_performance(fields)
            processing_data = await self._processing_report(items, fields)
            result = {"META": processing_data}
            return result

        except Exception as e:
            logger.info(e)
            return {"META": []}

    async def _check_account_auth(self):
        response = await self.client.verify_account_in_list()
        return response

    def _extract_action_value(self, actions, action_type):
        if not actions or not isinstance(actions, list):
            return "0"

        for action in actions:
            if action.get("action_type") == action_type:
                return action.get("value", "0")

        return "0"

    async def _processing_report(self, items, fields):
        result = []
        for item in items:
            row = {}
            for field in fields:
                if field == "date_start":
                    row["date"] = item.get(field)
                elif field == "video_play_actions":
                    # video_play_actions에서 video_view 값 추출
                    row["video_views"] = self._extract_action_value(
                        item.get(field), "video_view"
                    )
                else:
                    row[field] = item.get(field)

            result.append(row)
        return result
