import pandas as pd
import logging
from typing import Optional
from io import StringIO
from typing import List, Dict, Any
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class CSVService:
    """CSV 파일 처리 및 BigQuery 삽입 서비스"""

    def __init__(self, bigquery_client, gcs_client=None):
        self.bigquery_client = bigquery_client
        self.gcs_client = gcs_client

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

    async def upload_csv_via_gcs_to_bigquery(
        self,
        dataset_id: str,
        table_id: str,
        file_content: bytes,
        filename: str,
        schema: Optional[List] = None,
        required_columns: Optional[List[str]] = None,
        truncate: bool = True
    ) -> Dict[str, Any]:
        """
        CSV 파일을 GCS에 업로드 후 BigQuery 테이블에 로드
        큰 파일에 적합한 방식

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            file_content: CSV 파일 내용
            filename: 원본 파일명
            schema: BigQuery 스키마 (선택, 없으면 자동 생성)
            required_columns: 필수 컬럼 리스트 (선택)
            truncate: True이면 기존 데이터 삭제 후 삽입

        Returns:
            Dict: 업로드 결과
        """
        if not self.gcs_client:
            raise Exception("GCS 클라이언트가 설정되지 않았습니다")

        gcs_uri = None
        blob_name = None

        try:
            # 1. CSV 파일 파싱 및 검증
            logger.info(f"CSV 파일 파싱 시작...")
            data = await self.parse_csv_file(file_content)

            logger.info(f"CSV 데이터 검증 시작...")
            await self.validate_csv_data(data, required_columns)

            # 2. GCS에 파일 업로드
            logger.info(f"GCS에 파일 업로드 시작...")
            blob_name = self.gcs_client.generate_blob_name(dataset_id, table_id, filename)
            gcs_uri = await self.gcs_client.upload_file(file_content, blob_name)

            # 3. 데이터셋 생성 (없으면)
            logger.info(f"데이터셋 확인/생성: {dataset_id}")
            await self.bigquery_client.create_dataset(dataset_id)

            # 4. 테이블 truncate (옵션)
            if truncate:
                logger.info(f"테이블 truncate: {dataset_id}.{table_id}")
                await self.bigquery_client.truncate_table(dataset_id, table_id)

            # 5. GCS에서 BigQuery로 로드
            logger.info(f"GCS에서 BigQuery로 데이터 로드 시작: {gcs_uri}")
            await self._load_from_gcs_to_bigquery(
                dataset_id=dataset_id,
                table_id=table_id,
                gcs_uri=gcs_uri,
                schema=schema
            )

            # 6. GCS 파일 삭제 (선택적)
            logger.info(f"GCS 임시 파일 삭제: {gcs_uri}")
            await self.gcs_client.delete_file(blob_name)

            logger.info(f"CSV 업로드 완료: {dataset_id}.{table_id}")

            return {
                "status": "success",
                "message": f"CSV 파일 업로드 완료: {len(data)}행",
                "gcs_uri": gcs_uri
            }

        except Exception as e:
            logger.error(f"CSV 업로드 실패: {str(e)}")
            # 오류 발생 시 GCS 파일 정리
            if blob_name:
                try:
                    await self.gcs_client.delete_file(blob_name)
                except:
                    pass
            raise Exception(f"CSV 업로드 실패: {str(e)}")

    async def upload_csv_stream_via_gcs_to_bigquery(
        self,
        dataset_id: str,
        table_id: str,
        file_obj,
        filename: str,
        schema: Optional[List] = None,
        truncate: bool = True,
        validate_columns: bool = False
    ) -> Dict[str, Any]:
        """
        CSV 파일을 스트리밍 방식으로 GCS에 업로드 후 BigQuery 테이블에 로드
        매우 큰 파일에 적합 (메모리 효율적)

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            file_obj: 파일 객체 (file-like object)
            filename: 원본 파일명
            schema: BigQuery 스키마 (선택, 없으면 자동 생성)
            truncate: True이면 기존 데이터 삭제 후 삽입
            validate_columns: 컬럼 검증 여부 (큰 파일에서는 False 권장)

        Returns:
            Dict: 업로드 결과
        """
        if not self.gcs_client:
            raise Exception("GCS 클라이언트가 설정되지 않았습니다")

        gcs_uri = None
        blob_name = None

        try:
            # 1. GCS에 파일 스트리밍 업로드 (메모리에 로드하지 않음)
            logger.info(f"GCS에 파일 스트리밍 업로드 시작...")
            blob_name = self.gcs_client.generate_blob_name(dataset_id, table_id, filename)
            gcs_uri = await self.gcs_client.upload_file_stream(file_obj, blob_name)
            return {}
            # 2. 데이터셋 생성 (없으면)
            logger.info(f"데이터셋 확인/생성: {dataset_id}")
            await self.bigquery_client.create_dataset(dataset_id)

            # 3. 테이블 truncate (옵션)
            if truncate:
                logger.info(f"테이블 truncate: {dataset_id}.{table_id}")
                await self.bigquery_client.truncate_table(dataset_id, table_id)

            # 4. GCS에서 BigQuery로 로드
            logger.info(f"GCS에서 BigQuery로 데이터 로드 시작: {gcs_uri}")
            await self._load_from_gcs_to_bigquery(
                dataset_id=dataset_id,
                table_id=table_id,
                gcs_uri=gcs_uri,
                schema=schema
            )

            # 5. GCS 파일 삭제 (선택적)
            logger.info(f"GCS 임시 파일 삭제: {gcs_uri}")
            await self.gcs_client.delete_file(blob_name)

            logger.info(f"CSV 스트리밍 업로드 완료: {dataset_id}.{table_id}")

            return {
                "status": "success",
                "message": f"CSV 파일 스트리밍 업로드 완료",
                "gcs_uri": gcs_uri
            }

        except Exception as e:
            logger.error(f"CSV 스트리밍 업로드 실패: {str(e)}")
            # 오류 발생 시 GCS 파일 정리
            if blob_name:
                try:
                    await self.gcs_client.delete_file(blob_name)
                except:
                    pass
            raise Exception(f"CSV 스트리밍 업로드 실패: {str(e)}")

    async def _load_from_gcs_to_bigquery(
        self,
        dataset_id: str,
        table_id: str,
        gcs_uri: str,
        schema: Optional[List] = None
    ):
        """
        GCS에서 BigQuery로 데이터 로드

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            gcs_uri: GCS URI (gs://bucket/path)
            schema: BigQuery 스키마
        """
        try:
            client = await self.bigquery_client._get_client()
            table_ref = client.dataset(dataset_id).table(table_id)

            # 테이블이 없으면 생성
            if not await self.bigquery_client._table_exists(dataset_id, table_id):
                if schema:
                    await self.bigquery_client._create_table(dataset_id, table_id, schema)
                else:
                    raise Exception("테이블이 존재하지 않으며 스키마가 제공되지 않았습니다")

            # 로드 작업 설정
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # 헤더 스킵
                autodetect=False if schema else True,  # 스키마가 있으면 자동 감지 비활성화
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                max_bad_records=0,  # 오류 허용 안 함
                allow_quoted_newlines=True,  # CSV 내 줄바꿈 허용
                allow_jagged_rows=False,  # 불규칙한 행 허용 안 함
            )

            # GCS에서 BigQuery로 로드
            load_job = client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config
            )

            # 작업 완료 대기
            load_job.result()

            logger.info(f"GCS에서 BigQuery로 로드 완료: {dataset_id}.{table_id}")

        except Exception as e:
            logger.error(f"GCS에서 BigQuery 로드 실패: {str(e)}")
            raise Exception(f"GCS에서 BigQuery 로드 실패: {str(e)}")