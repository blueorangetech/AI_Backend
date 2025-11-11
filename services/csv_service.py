import pandas as pd
import logging
from typing import Optional
from io import StringIO
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class CSVService:
    """CSV 파일 처리 및 BigQuery 삽입 서비스"""

    def __init__(self, bigquery_client):
        self.bigquery_client = bigquery_client

    async def parse_csv_file(self, file_content: bytes) -> list:
        """
        CSV 파일을 파싱하여 딕셔너리 리스트로 변환

        Args:
            file_content: CSV 파일 내용 (bytes)

        Returns:
            List[Dict]: 파싱된 데이터
        """
        try:
            # bytes를 문자열로 변환
            csv_string = file_content.decode('utf-8')

            # pandas로 CSV 파싱
            df = pd.read_csv(StringIO(csv_string))

            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)

            # DataFrame을 딕셔너리 리스트로 변환
            data = df.to_dict(orient='records')

            logger.info(f"CSV 파일 파싱 완료: {len(data)}행, {len(df.columns)}열")
            logger.info(f"컬럼: {list(df.columns)}")

            return data

        except Exception as e:
            logger.error(f"CSV 파일 파싱 실패: {str(e)}")
            raise Exception(f"CSV 파일 파싱 실패: {str(e)}")

    async def validate_csv_data(self, data: List[Dict[str, Any]], 
                                required_columns: Optional[List[str]] = None) -> bool:
        """
        CSV 데이터 검증

        Args:
            data: 검증할 데이터
            required_columns: 필수 컬럼 리스트 (선택)

        Returns:
            bool: 검증 성공 여부
        """
        if not data:
            raise Exception("CSV 파일에 데이터가 없습니다")

        # 필수 컬럼 체크
        if required_columns:
            actual_columns = set(data[0].keys())
            missing_columns = set(required_columns) - actual_columns

            if missing_columns:
                raise Exception(f"필수 컬럼이 누락되었습니다: {missing_columns}")

        logger.info(f"CSV 데이터 검증 완료: {len(data)}행")
        return True

    async def upload_csv_to_bigquery(
        self,
        dataset_id: str,
        table_id: str,
        file_content: bytes,
        schema: Optional[List] = None,
        required_columns: Optional[List[str]] = None,
        truncate: bool = True
    ) -> Dict[str, Any]:
        """
        CSV 파일을 BigQuery 테이블에 업로드

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            file_content: CSV 파일 내용
            schema: BigQuery 스키마 (선택, 없으면 자동 생성)
            required_columns: 필수 컬럼 리스트 (선택)
            truncate: True이면 기존 데이터 삭제 후 삽입

        Returns:
            Dict: 업로드 결과
        """
        try:
            # 1. CSV 파일 파싱
            logger.info(f"CSV 파일 파싱 시작...")
            data = await self.parse_csv_file(file_content)

            # 2. 데이터 검증
            logger.info(f"CSV 데이터 검증 시작...")
            await self.validate_csv_data(data, required_columns)

            # 3. 데이터셋 생성 (없으면)
            logger.info(f"데이터셋 확인/생성: {dataset_id}")
            await self.bigquery_client.create_dataset(dataset_id)

            # 4. 테이블 truncate (옵션)
            if truncate:
                logger.info(f"테이블 truncate: {dataset_id}.{table_id}")
                await self.bigquery_client.truncate_table(dataset_id, table_id)

            # 5. 데이터 삽입
            logger.info(f"BigQuery 데이터 삽입 시작: {len(data)}행")
            await self.bigquery_client.insert_start(dataset_id, table_id, schema, data)

            logger.info(f"CSV 업로드 완료: {dataset_id}.{table_id}")

            return {
                "status": "success",
                "message": f"CSV 파일 업로드 완료: {len(data)}행"
            }

        except Exception as e:
            logger.error(f"CSV 업로드 실패: {str(e)}")
            raise Exception(f"CSV 업로드 실패: {str(e)}")