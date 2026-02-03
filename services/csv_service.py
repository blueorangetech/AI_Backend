import pandas as pd
import logging, re
from typing import Optional
from io import StringIO, BytesIO
from typing import List, Dict, Any
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class CSVService:
    """CSV 파일 처리 및 BigQuery 삽입 서비스"""

    def __init__(self, bigquery_client, gcs_client=None):
        self.bigquery_client = bigquery_client
        self.gcs_client = gcs_client

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

    def remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터프레임에서 이상치 제거
        """
        # 정상 형식 패턴: YYYY-MM-DDTHH:MM:SS.000+09:00
        normal_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{2}:\d{2}$'

        # 비정상 형식 행 찾기
        abnormal_indices = []
        for idx, value in enumerate(df['site_owner_join_time']):
            # NaN이 아니고, 정상 패턴에 맞지 않는 경우
            if pd.notna(value) and not re.match(normal_pattern, str(value)):
                abnormal_indices.append(idx)
                print(f"비정상 형식 발견 (행 {idx}): {value}")

        # 비정상 형식 행 제거
        df = df.drop(abnormal_indices)

        logger.info(f"이상치 제거 전 데이터: {len(df)}행")

        # 예시: 특정 컬럼의 특정 값을 가진 행 제거
        # df_cleaned = df[df['your_column'] != 'outlier_value']
        df_cleaned = df.copy()

        logger.info(f"이상치 제거 후 데이터: {len(df_cleaned)}행 (제거된 행: {len(df) - len(df_cleaned)})")

        return df_cleaned
    
    async def upload_file_direct(
        self,
        dataset_id: str,
        table_id: str,
        file_content: bytes,
        filename: str,
        schema: Optional[List] = None,
        truncate: bool = True,
        processor_func: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        파일을 서버에서 직접 BigQuery로 업로드 (GCS 경유 안 함)
        
        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            file_content: 파일 내용 (bytes)
            filename: 파일명 (확장자 확인용)
            schema: BigQuery 스키마
            truncate: 기존 데이터 삭제 여부
            processor_func: 데이터프레임 가공 함수 (선택 사항)
        """
        try:
            # 1. 파일 형식 확인 및 읽기
            logger.info(f"직접 업로드 처리 시작: {filename}")
            
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(BytesIO(file_content))
            elif filename.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(BytesIO(file_content))
            else:
                # 기본값 CSV
                df = pd.read_csv(BytesIO(file_content))
            
            # 2. 데이터 가공 로직 적용 (전달된 함수가 있을 경우)
            if processor_func:
                logger.info(f"데이터 가공 로직 적용 중...")
                df = processor_func(df)
            
            logger.info(f"데이터 로드 완료: {len(df)}행")
            
            # 2. 데이터셋 생성 (없으면)
            await self.bigquery_client.create_dataset(dataset_id)
            
            # 3. BigQuery에 삽입 (truncate 여부를 함께 전달)
            # dict 리스트로 변환
            rows = df.to_dict(orient='records')
            
            # BigQueryClient의 insert_start 사용 (이제 truncate 파라미터를 지원함)
            result = await self.bigquery_client.insert_start(
                dataset_id=dataset_id,
                table_id=table_id,
                schema=schema,
                rows=rows,
                truncate=truncate
            )
            
            if result.get("status") == "success":
                return {
                    "status": "success",
                    "message": f"BigQuery 업로드 완료: {len(df)}행",
                    "rows_inserted": len(df)
                }
            else:
                raise Exception(result.get("message", "BigQuery 삽입 실패"))

        except Exception as e:
            logger.error(f"직접 업로드 중 오류 발생: {str(e)}")
            raise Exception(f"BigQuery 직접 업로드 실패: {str(e)}")


    async def process_gcs_file_with_outlier_removal(
        self,
        dataset_id: str,
        table_id: str,
        blob_name: str,
        schema: Optional[List] = None,
        truncate: bool = True
    ) -> Dict[str, Any]:
        """
        GCS에 이미 업로드된 CSV 파일을 처리하여 BigQuery에 로드
        (클라이언트가 GCS Signed URL로 직접 업로드한 경우)

        **메모리 최적화**: 대용량 데이터 처리를 위해 GCS Load Job 사용

        처리 과정:
        1. GCS에서 파일을 다운로드
        2. pandas DataFrame으로 변환
        3. 이상치 제거 로직 적용
        4. 행 수 저장 및 메모리 정리
        5. GCS 원본 파일 삭제
        6. 데이터셋 생성 (없으면)
        7. 테이블 truncate (옵션)
        8. 정제된 데이터를 CSV로 변환하여 GCS에 업로드
        9. GCS에서 BigQuery로 Load Job 실행 (메모리 효율적)
        10. GCS 정제된 파일 삭제

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            blob_name: GCS blob 경로
            schema: BigQuery 스키마 (선택)
            truncate: True이면 기존 데이터 삭제 후 삽입
            processor_func: 데이터프레임 가공 함수 (선택 사항)

        Returns:
            Dict: 업로드 결과
        """
        if not self.gcs_client:
            raise Exception("GCS 클라이언트가 설정되지 않았습니다")

        try:
            # 1. GCS에서 파일 다운로드
            logger.info(f"GCS에서 파일 다운로드 중: {blob_name}")
            file_content = await self.gcs_client.download_file(blob_name)

            # 2. pandas DataFrame으로 변환
            logger.info(f"CSV 파일을 DataFrame으로 변환 중...")
            df = pd.read_csv(io.BytesIO(file_content))
            
            # 데이터 가공 로직 적용
            if processor_func:
                logger.info("데이터 가공 로직 적용 중...")
                df = processor_func(df)
                
            logger.info(f"원본 데이터: {len(df)}행, {len(df.columns)}열")

            # 3. 이상치 제거 (기본 로직 유지)
            logger.info(f"이상치 제거 로직 적용 중...")
            df_cleaned = self.remove_outliers(df)

            # 행 수 저장 (메모리 정리 전)
            original_rows = len(df)
            cleaned_rows = len(df_cleaned)

            # 4. GCS 원본 파일 삭제
            logger.info(f"GCS 원본 파일 삭제: {blob_name}")
            await self.gcs_client.delete_file(blob_name)

            # 5. 데이터셋 생성 (없으면)
            logger.info(f"데이터셋 확인/생성: {dataset_id}")
            await self.bigquery_client.create_dataset(dataset_id)

            # 6. 테이블 truncate (옵션)
            if truncate:
                logger.info(f"테이블 truncate: {dataset_id}.{table_id}")
                await self.bigquery_client.truncate_table(dataset_id, table_id)

            # 7. 정제된 데이터를 CSV로 변환하여 GCS에 업로드 (메모리 효율적)
            logger.info(f"정제된 데이터를 CSV로 변환 중...")
            csv_buffer = StringIO()
            df_cleaned.to_csv(csv_buffer, index=False)
            cleaned_csv_bytes = csv_buffer.getvalue().encode('utf-8')

            # 메모리 정리
            del df
            del df_cleaned
            del csv_buffer

            # 정제된 CSV를 GCS에 업로드
            cleaned_blob_name = f"cleaned_{blob_name}"
            logger.info(f"정제된 데이터를 GCS에 업로드 중: {cleaned_blob_name}")
            cleaned_gcs_uri = await self.gcs_client.upload_file(
                cleaned_csv_bytes, cleaned_blob_name
            )

            # 메모리 정리
            del cleaned_csv_bytes

            # 8. GCS에서 BigQuery로 로드 (메모리 효율적)
            logger.info(f"GCS에서 BigQuery로 데이터 로드 중: {cleaned_gcs_uri}")
            await self._load_from_gcs_to_bigquery(
                dataset_id=dataset_id,
                table_id=table_id,
                gcs_uri=cleaned_gcs_uri,
                schema=schema
            )

            # 9. GCS 정제된 파일 삭제
            logger.info(f"GCS 정제된 파일 삭제: {cleaned_gcs_uri}")
            await self.gcs_client.delete_file(cleaned_blob_name)

            logger.info(f"GCS 파일 처리 후 BigQuery 업로드 완료: {dataset_id}.{table_id}")

            return {
                "status": "success",
                "message": f"BigQuery 업로드 완료 : {cleaned_rows} 행",
                "original_rows": original_rows,
                "cleaned_rows": cleaned_rows,
                "removed_rows": original_rows - cleaned_rows
            }

        except Exception as e:
            logger.error(f"GCS 파일 처리 실패: {str(e)}")
            # 오류 발생 시 GCS 파일 정리 시도
            try:
                await self.gcs_client.delete_file(blob_name)
            except:
                pass
            raise Exception(f"GCS 파일 처리 실패: {str(e)}")