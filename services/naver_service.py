from clients.naver_api_client import NaverAPIClient
from configs.naver_config import naver_field_master, naver_master_config, naver_vaild_fields
from models.bigquery_schemas import naver_search_ad_schema, naver_search_ad_cov_schema, naver_shopping_ad_schema, naver_shopping_ad_cov_schema
import os, io, csv
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

    async def create_complete_report(self, master_list: list, stat_types: list) -> dict:
        """완전한 네이버 리포트 생성 (마스터 + 통계 데이터)"""
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
            result = self._merge_reports(file_list, master_list, stat_types)

            return result

        finally:
            # 임시 파일 정리
            self._cleanup_files(file_list)

    async def _create_and_download_master_report(self, master_type: str) -> str:
        """마스터 리포트 생성 및 다운로드"""
        # 1. 리포트 생성 요청
        report_id = await self.client.create_master_report(master_type)

        # 2. 완료될 때까지 대기
        download_url = await self._wait_for_report_completion(
            "/master-reports", report_id
        )

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
        download_url = await self._wait_for_report_completion(
            "/stat-reports", report_id
        )

        # 3. 리포트 다운로드 URL 요청
        url_request = await self.client.request_download_url(download_url)

        # 4. URL로 다운로드 진행
        file_path = await self._file_download(url_request, stat_type)

        # 5. 서버에서 리포트 삭제
        await self.client.delete_report("/stat-reports", report_id)

        return file_path

    async def _wait_for_report_completion(
        self, uri: str, report_id: str, max_attempts: int = 60
    ) -> str:
        """리포트 완료까지 대기"""
        for attempt in range(max_attempts):
            download_url = await self.client.get_report_status(uri, report_id)
            if download_url["status"] == "BUILT":
                return download_url["downloadUrl"]

            time.sleep(0.7)

        raise Exception(f"리포트 생성 시간 초과: {uri}/{report_id}")

    async def _file_download(self, url_request, file_name) -> str:
        """다운로드 URL로 진행"""
        if url_request.status_code != 200:
            raise Exception(f"다운로드 실패: {url_request.status_code}")

        # 파일 저장
        tsv_data = url_request.content.decode("utf-8")
        df = pd.read_csv(io.StringIO(tsv_data), delimiter="\t", quoting=csv.QUOTE_NONE)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{file_name}_report_{timestamp}.csv"
        file_path = os.path.join(self.download_dir, file_name)

        df.to_csv(file_path, index=False, encoding="utf-8")

        return file_path

    def _merge_reports(self, report_list: list, master_list: list, stat_types: list) -> dict:
        """여러 리포트 파일을 병합하여 하나의 DataFrame으로 만들기"""
        keys = master_list + stat_types
        header_list = {}

        # 마스터 리포트를 header_list 변수에 추가
        for key in keys:
            for report in report_list:
                report_name = f"{key}_report"
                if report_name in report:
                    header_list[key] = naver_field_master[report_name]
                    break
        
        logger.info(f"Master Report Field Define Complete")

        # 각 파일을 DataFrame으로 읽기
        data ={}
        
        # 스키마 매핑
        schema_map = {
            'AD': naver_search_ad_schema(),
            'AD_CONVERSION': naver_search_ad_cov_schema(),
            'SHOPPINGKEYWORD_DETAIL': naver_shopping_ad_schema(),
            'SHOPPINGKEYWORD_CONVERSION_DETAIL': naver_shopping_ad_cov_schema()
        }
        
        for key, report in zip(keys, report_list):
            df = pd.read_csv(report, header=None, names=header_list[key], 
                           low_memory=False, index_col=False)
            
            # 해당 스키마의 INTEGER 필드 찾기
            if key in schema_map:
                integer_fields = [k for k, v in schema_map[key].items() 
                                if v == 'INTEGER']
                
                # INTEGER 필드를 정수로 변환
                for col in integer_fields:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            data[key] = df    
        logger.info(f"Report Load Complete")

        # 마스터 데이터를 딕셔너리로 변환
        master_dict = {}
        
        # naver_config.py 에서 id: name 데이터 불러오기
        for master_name in master_list:
            config_data = naver_master_config[master_name]
            id, name = config_data["id"], config_data["name"]
            
            name = name if isinstance(name, list) else [name]
            
            filtered_data = data[master_name].dropna(subset=name)
            master_dict[master_name] = filtered_data.set_index(id)[name]
            logger.info(f"{master_name} Index Complete")

        logger.info(f"Index Data Craft Complete")
        
        # Stat 리포트와 Master 리포트 연결 - ID에 이름 부여
        result = {}
        for stat_type in stat_types:
            basics = ["Campaign", "Adgroup"]
            report = data[stat_type]

            # AD 리포트는 Keyword 사용
            if "AD" in stat_type:
                basics.append("Keyword")

            # Shopping 리포트는 Shopping 사용
            if "SHOPPING" in stat_type:
                basics.append("ShoppingProduct")

            for basic in basics:
                config_data = naver_master_config[basic]
                id, name = config_data["id"], config_data["name"]
                # report[name] = report[id].map(master_dict[basic]).fillna("-")
                report = pd.merge(report, master_dict[basic], on=id, how="left").fillna("-")
                
            report["date"] = pd.to_datetime(report["date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
            
            valid_header = naver_vaild_fields[stat_type]
            report = report[valid_header]
            report = report.to_dict("records")
            result[f"NAVER_{stat_type}"] = report
            basics = ["Campaign", "Adgroup"] # basics 초기화

        return result

    def _cleanup_files(self, file_list: list):
        """임시 파일들 정리"""
        for file_path in file_list:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"파일 삭제 실패: {file_path}, 에러: {e}")
