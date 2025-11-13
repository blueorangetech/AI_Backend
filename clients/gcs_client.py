from google.cloud import storage
from utils.gcs_client_manager import get_gcs_client
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class GCSClient:
    def __init__(self, config, bucket_name):
        self.config = config
        self.bucket_name = bucket_name
        self._client = None

    async def _get_client(self):
        """GCS 클라이언트 반환 (매니저를 통해)"""
        if self._client is None:
            self._client = await get_gcs_client(self.config)
        return self._client

    async def upload_file(self, file_content: bytes, destination_blob_name: str) -> str:
        """
        파일을 GCS에 업로드 (바이트 데이터)

        Args:
            file_content: 업로드할 파일 내용 (bytes)
            destination_blob_name: GCS에 저장될 파일 경로

        Returns:
            str: GCS URI (gs://bucket/path)
        """
        try:
            client = await self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(destination_blob_name)

            # 파일 업로드
            blob.upload_from_string(file_content, content_type='text/csv')

            gcs_uri = f"gs://{self.bucket_name}/{destination_blob_name}"
            logger.info(f"File uploaded to {gcs_uri}")

            return gcs_uri

        except Exception as e:
            logger.error(f"Failed to upload file to GCS: {str(e)}")
            raise Exception(f"GCS 파일 업로드 실패: {str(e)}")

    async def upload_file_stream(self, file_obj, destination_blob_name: str) -> str:
        """
        파일을 GCS에 스트리밍 업로드 (메모리 효율적)

        Args:
            file_obj: 파일 객체 (file-like object)
            destination_blob_name: GCS에 저장될 파일 경로

        Returns:
            str: GCS URI (gs://bucket/path)
        """
        try:
            client = await self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(destination_blob_name)

            # 스트리밍 업로드 (청크 단위로 처리)
            blob.upload_from_file(file_obj, content_type='text/csv', rewind=True)

            gcs_uri = f"gs://{self.bucket_name}/{destination_blob_name}"
            logger.info(f"File streamed to {gcs_uri}")

            return gcs_uri

        except Exception as e:
            logger.error(f"Failed to stream file to GCS: {str(e)}")
            raise Exception(f"GCS 파일 스트리밍 업로드 실패: {str(e)}")

    async def delete_file(self, blob_name: str) -> bool:
        """
        GCS에서 파일 삭제

        Args:
            blob_name: 삭제할 파일 경로

        Returns:
            bool: 삭제 성공 여부
        """
        try:
            client = await self._get_client()
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)

            blob.delete()

            logger.info(f"File deleted from GCS: gs://{self.bucket_name}/{blob_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete file from GCS: {str(e)}")
            return False

    async def list_files(self, prefix: str = "") -> list:
        """
        GCS 버킷의 파일 목록 조회

        Args:
            prefix: 파일 경로 프리픽스 (폴더 경로)

        Returns:
            list: 파일 이름 리스트
        """
        try:
            client = await self._get_client()
            bucket = client.bucket(self.bucket_name)

            blobs = bucket.list_blobs(prefix=prefix)
            file_list = [blob.name for blob in blobs]

            logger.info(f"Found {len(file_list)} files in gs://{self.bucket_name}/{prefix}")
            return file_list

        except Exception as e:
            logger.error(f"Failed to list files from GCS: {str(e)}")
            return []

    def generate_blob_name(self, dataset_id: str, table_id: str, filename: str) -> str:
        """
        GCS blob 이름 생성 (폴더 구조 포함)

        Args:
            dataset_id: BigQuery 데이터셋 ID
            table_id: BigQuery 테이블 ID
            filename: 원본 파일명

        Returns:
            str: blob 이름 (예: csv_uploads/dataset/table/2023-12-01_filename.csv)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{dataset_id}/{table_id}/{timestamp}_{filename}"
