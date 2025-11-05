from google.cloud import bigquery
from google.oauth2 import service_account
import asyncio
import logging
from typing import Optional, Dict
import json

logger = logging.getLogger(__name__)


class BigQueryClientManager:
    """
    BigQuery 클라이언트 매니저
    설정별로 클라이언트를 싱글톤으로 관리하여 메모리 효율성 향상
    """
    _instances: Dict[str, bigquery.Client] = {}
    _lock = asyncio.Lock()

    @classmethod
    async def get_client(cls, config: dict) -> bigquery.Client:
        """
        설정 기반으로 BigQuery 클라이언트 반환
        동일한 설정이면 기존 클라이언트 재사용
        """
        # 설정을 기반으로 고유 키 생성
        config_key = cls._generate_config_key(config)

        if config_key not in cls._instances:
            async with cls._lock:
                if config_key not in cls._instances:
                    credentials = service_account.Credentials.from_service_account_info(config)
                    client = bigquery.Client(
                        credentials=credentials,
                        project=credentials.project_id,
                        # 성능 최적화 설정
                        default_query_job_config=bigquery.QueryJobConfig(
                            use_query_cache=True,
                            maximum_bytes_billed=10**12  # 1TB 제한
                        )
                    )
                    cls._instances[config_key] = client
                    logger.info(f"새로운 BigQuery 클라이언트가 생성되었습니다. (프로젝트: {credentials.project_id})")

        return cls._instances[config_key]

    @classmethod
    def _generate_config_key(cls, config: dict) -> str:
        """설정 기반으로 고유 키 생성"""
        # 중요한 필드들만 사용하여 키 생성
        key_fields = {
            'project_id': config.get('project_id'),
            'client_email': config.get('client_email'),
            'private_key_id': config.get('private_key_id')
        }
        return json.dumps(key_fields, sort_keys=True)

    @classmethod
    async def close_all(cls):
        """모든 BigQuery 클라이언트 정리"""
        if cls._instances:
            async with cls._lock:
                for config_key, client in cls._instances.items():
                    try:
                        client.close()
                        logger.info(f"BigQuery 클라이언트가 정리되었습니다: {config_key}")
                    except Exception as e:
                        logger.warning(f"BigQuery 클라이언트 정리 중 오류: {e}")

                cls._instances.clear()
                logger.info("모든 BigQuery 클라이언트가 정리되었습니다.")

    @classmethod
    async def close_client(cls, config: dict):
        """특정 설정의 BigQuery 클라이언트 정리"""
        config_key = cls._generate_config_key(config)

        if config_key in cls._instances:
            async with cls._lock:
                if config_key in cls._instances:
                    try:
                        cls._instances[config_key].close()
                        del cls._instances[config_key]
                        logger.info(f"BigQuery 클라이언트가 정리되었습니다")
                    except Exception as e:
                        logger.warning(f"BigQuery 클라이언트 정리 중 오류: {e}")

    @classmethod
    def get_client_count(cls) -> int:
        """현재 관리 중인 클라이언트 수 반환"""
        return len(cls._instances)


# 전역 함수들
async def get_bigquery_client(config: dict) -> bigquery.Client:
    """전역 BigQuery 클라이언트 반환"""
    return await BigQueryClientManager.get_client(config)


async def cleanup_all_bigquery_clients():
    """모든 BigQuery 클라이언트 정리"""
    await BigQueryClientManager.close_all()