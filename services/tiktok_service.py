from clients.tiktok_api_client import TikTokAPIClient
import pandas as pd
import time, datetime, logging

logger = logging.getLogger(__name__)


class TikTokReportService:
    def __init__(self, tiktok_client: TikTokAPIClient):
        self.client = tiktok_client

    async def create_report(self, dimensions_list, metrics_list):
        all_data = []
        page = 1
        page_size = 1000  # 최대값 설정

        while True:
            response = await self.client.get_reports(page=page, page_size=page_size,
                                                     dimensions_list=dimensions_list, metrics_list=metrics_list)

            # 에러 체크
            if "error" in response or response.get("code") != 0:
                logger.error(f"TikTok API error: {response}")
                break

            # 데이터 추출
            data = response.get("data", {})
            list_data = data.get("list", [])
            page_info = data.get("page_info", {})

            if list_data:
                # dimensions와 metrics 합치기
                flattened_data = []
                for record in list_data:
                    flat_record = {}
                    # dimensions 추가
                    if "dimensions" in record:
                        flat_record.update(record["dimensions"])
                    # metrics 추가
                    if "metrics" in record:
                        flat_record.update(record["metrics"])
                    flat_record["date"] = flat_record.pop("stat_time_day")
                    
                    flattened_data.append(flat_record)

                all_data.extend(flattened_data)
                logger.info(f"Fetched page {page}/{page_info.get('total_page', 1)}, records: {len(list_data)}")

            # 마지막 페이지 확인
            if page >= page_info.get("total_page", 1):
                break

            page += 1

        result = {"TIKTOK": all_data}
        return result