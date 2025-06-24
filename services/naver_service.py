from clients.naver_api_client import NaverAPIClient
from reports.report_fields import (naver_campaign_fields, naver_ad_group_fields, 
                            naver_keyword_fields, naver_ad_fields, naver_vaild_fields)
import os, io
import pandas as pd
import time, datetime

class NaverReportService:
    """네이버 리포트 관련 비즈니스 로직을 처리하는 서비스"""
    def __init__(self, naver_client: NaverAPIClient):
        self.client = naver_client
        self.service_dir = os.path.dirname(os.path.abspath(__file__))
        self.download_dir = os.path.join(self.service_dir, "download")

        os.makedirs(self.download_dir, exist_ok=True)

    async def create_complete_report(self, target_date: str = "20250121") -> pd.DataFrame:
        """완전한 네이버 리포트 생성 (마스터 + 통계 데이터)"""
        master_list = ["Campaign", "Adgroup", "Keyword"]
        stat_list = ["AD"]
        file_list = []
        
        try:
            # 마스터 리포트 생성
            for master in master_list:
                file_path = await self._create_and_download_master_report(master)
                file_list.append(file_path)
            
            # 통계 리포트 생성
            for stat in stat_list:
                file_path = await self._create_and_download_stat_report(stat, target_date)
                file_list.append(file_path)
            
            # 리포트 병합
            result = self._merge_reports(file_list)
            
            return result
        
        finally:
            # 임시 파일 정리
            self._cleanup_files(file_list)
    
    async def _create_and_download_master_report(self, master_type: str) -> str:
        """마스터 리포트 생성 및 다운로드"""
        # 1. 리포트 생성 요청
        report_id = await self.client.create_master_report(master_type)
        
        # 2. 완료될 때까지 대기
        download_url = await self._wait_for_report_completion("/master-reports", report_id)
        
        # 3. 리포트 다운로드 URL 요청
        url_request = await self.client.request_download_url(download_url)

        # 4. URL로 다운로드 진행
        file_path = await self._file_download(url_request, master_type)
        
        # 5. 서버에서 리포트 삭제
        await self.client.delete_report("/master-reports", report_id)

        return file_path
    
    async def _create_and_download_stat_report(self, stat_type: str, target_date: str) -> str:
        """통계 리포트 생성 및 다운로드"""
        # 1. 리포트 생성 요청
        report_id = await self.client.create_stat_report(stat_type, target_date)
        
        # 2. 완료될 때까지 대기
        download_url = await self._wait_for_report_completion("/stat-reports", report_id)
        
        # 3. 리포트 다운로드 URL 요청
        url_request = await self.client.request_download_url(download_url)
        
        # 4. URL로 다운로드 진행
        file_path = await self._file_download(url_request, stat_type)

        # 5. 서버에서 리포트 삭제
        await self.client.delete_report("/stat-reports", report_id)

        return file_path
    
    async def _wait_for_report_completion(self, uri: str, report_id: str, max_attempts: int = 10) -> str:
        """리포트 완료까지 대기"""
        for attempt in range(max_attempts):
            download_url = await self.client.get_report_status(uri, report_id)
            
            if download_url["status"] == "BUILT":
                return download_url["downloadUrl"]
            
            time.sleep(0.5)
        
        raise Exception(f"리포트 생성 시간 초과: {uri}/{report_id}")
    
    async def _file_download(self, url_request, file_name) -> str:
        """다운로드 URL로 진행"""
        if url_request.status_code != 200:
            raise Exception(f"다운로드 실패: {url_request.status_code}")
        
        # 파일 저장
        tsv_data = url_request.content.decode("utf-8")
        df = pd.read_csv(io.StringIO(tsv_data), delimiter="\t")

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"{file_name}_report_{timestamp}.csv"
        file_path = os.path.join(self.download_dir, file_name)
        
        df.to_csv(file_path, index=False, encoding="utf-8")

        return file_path
    
    def _merge_reports(self, report_list: list) -> pd.DataFrame:
        """여러 리포트 파일을 병합하여 하나의 DataFrame으로 만들기"""
        header_list = [
            naver_campaign_fields(), 
            naver_ad_group_fields(),
            naver_keyword_fields(), 
            naver_ad_fields()
        ]
        
        keys = ["campaign", "group", "keyword", "ad_result"]
        
        # 각 파일을 DataFrame으로 읽기
        data = {
            key: pd.read_csv(report, header=None, names=header)
            for key, report, header in zip(keys, report_list, header_list)
        }
        
        # 데이터 병합
        master_header = pd.merge(data["campaign"], data["group"], on="CampaignID", how="left")
        master_header = pd.merge(master_header, data["keyword"], on="AdGroupID", how="left")
        
        report = pd.merge(data["ad_result"], master_header, 
                         on=["CampaignID", "AdGroupID", "AdKeywordID"], how="left")
        
        # 필요한 컬럼만 선택
        valid_header = naver_vaild_fields()
        result = report[valid_header].fillna(0)
        
        # 결과 저장 (선택사항)
        file_path = os.path.join(self.download_dir, "naver_result.csv")
        result.to_csv(file_path, index=False, encoding="euc-kr")
        
        return result
    
    def _cleanup_files(self, file_list: list):
        """임시 파일들 정리"""
        for file_path in file_list:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"파일 삭제 실패: {file_path}, 에러: {e}")
