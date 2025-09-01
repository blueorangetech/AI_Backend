from clients.naver_api_client import NaverAPIClient
from configs.report_fields import (naver_campaign_fields, naver_ad_group_fields, 
                            naver_keyword_fields, naver_ad_fields, naver_ad_conversion_fields,
                            naver_vaild_fields, naver_conversion_vaild_fields)
import os, io
import pandas as pd
import time, datetime, logging

logger = logging.getLogger(__name__)

class NaverReportService:
    """네이버 리포트 관련 비즈니스 로직을 처리하는 서비스"""
    def __init__(self, naver_client: NaverAPIClient):
        self.client = naver_client
        self.service_dir = os.path.dirname(os.path.abspath(__file__))
        self.download_dir = os.path.join(self.service_dir, "download")

        os.makedirs(self.download_dir, exist_ok=True)

    async def create_complete_report(self, stat_types: list) -> dict:
        """완전한 네이버 리포트 생성 (마스터 + 통계 데이터)"""
        master_list = ["Campaign", "Adgroup", "Keyword"]
        file_list = []
        
        try:
            # 마스터 리포트 생성
            for master in master_list:
                file_path = await self._create_and_download_master_report(master)
                file_list.append(file_path)
                logger.info(f"{master} 정보 생성 완료")
            
            # 통계 리포트 생성
            for stat_type in stat_types:
                file_path = await self._create_and_download_stat_report(stat_type)
                file_list.append(file_path)
                logger.info(f"{stat_type} 정보 생성 완료")
            
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
    
    async def _create_and_download_stat_report(self, stat_type: str) -> str:
        """통계 리포트 생성 및 다운로드"""
        # 1. 리포트 생성 요청
        report_id = await self.client.create_stat_report(stat_type)
        
        # 2. 완료될 때까지 대기
        download_url = await self._wait_for_report_completion("/stat-reports", report_id)
        
        # 3. 리포트 다운로드 URL 요청
        url_request = await self.client.request_download_url(download_url)
        
        # 4. URL로 다운로드 진행
        file_path = await self._file_download(url_request, stat_type)

        # 5. 서버에서 리포트 삭제
        await self.client.delete_report("/stat-reports", report_id)

        return file_path
    
    async def _wait_for_report_completion(self, uri: str, report_id: str, max_attempts: int = 60) -> str:
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
    
    def _merge_reports(self, report_list: list) -> dict:
        """여러 리포트 파일을 병합하여 하나의 DataFrame으로 만들기"""
        header_list = [
            naver_campaign_fields(), 
            naver_ad_group_fields(),
            naver_keyword_fields(), 
            naver_ad_fields()
        ]

        keys = ["campaign", "group", "keyword", "ad_result"]

        # 프리미엄 로그분석 : 전환 데이터 추가 읽기 작업
        conversion_files = [f for f in report_list if "AD_CONVERSION_report" in f]
        if conversion_files:
            logger.info("프리미엄 로그분석 대상")
            header_list.append(naver_ad_conversion_fields())
            keys.append("conversion")

        # 각 파일을 DataFrame으로 읽기
        data = {
            key: pd.read_csv(report, header=None, names=header, low_memory=False)
            for key, report, header in zip(keys, report_list, header_list)
        }
        
        # 마스터 데이터를 딕셔너리로 변환
        campaign_dict = data["campaign"].set_index("campaignID")["campaignName"].to_dict()
        adgroup_dict = data["group"].set_index("adGroupID")["adGroupName"].to_dict()
        keyword_dict = data["keyword"].set_index("adKeywordID")["adKeyword"].to_dict()
        
        # 통계 데이터에 마스터 정보 매핑
        report = self._data_mapping(data["ad_result"], campaign_dict, adgroup_dict, keyword_dict)

        logger.info("기본 리포트 매핑 완료")
        report_header = naver_vaild_fields()
        
        # 필요한 컬럼만 선택
        basic_result = report[report_header].fillna(0)
        
        # Date 컬럼 형식 변환 (yyyymmdd -> yyyy-mm-dd)
        basic_result['date'] = pd.to_datetime(basic_result['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        basic = basic_result.to_dict("records")

        conversion = []
        
        # 프리미엄 로그분석: 데이터 추가
        if conversion_files:
            conversion_data = self._data_mapping(data["conversion"], campaign_dict, adgroup_dict, keyword_dict)

            conversion_header = naver_conversion_vaild_fields()
            conversion_result = conversion_data[conversion_header].fillna(0)

            conversion_result['date'] = pd.to_datetime(conversion_result['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            conversion = conversion_result.to_dict("records")            
            logger.info("프리미엄 로그분석 리포트 매핑 완료")
            
        result = {"naver_search_ad": basic, "naver_search_ad_conv": conversion}
        return result
    
    def _data_mapping(self, report, campaign_dict, adgroup_dict, keyword_dict):
        """ 리포트 데이터와 마스터 데이터를 매핑"""
        report["campaignName"] = report["campaignID"].map(campaign_dict)
        report["adGroupName"] = report["adGroupID"].map(adgroup_dict)
        report["adKeyword"] = report["adKeywordID"].map(keyword_dict)
        return report
    
    def _cleanup_files(self, file_list: list):
        """임시 파일들 정리"""
        for file_path in file_list:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"파일 삭제 실패: {file_path}, 에러: {e}")
