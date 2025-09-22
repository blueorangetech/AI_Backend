from pymongo import MongoClient
import os
import logging
from dotenv import load_dotenv
import asyncio
from typing import Optional

load_dotenv()
logger = logging.getLogger(__name__)


class MongoDB:
    _instance: Optional[MongoClient] = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls):
        """MongoDB 클라이언트 인스턴스 반환 (연결 상태 확인 포함)"""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls._create_client()
                    logger.info("새로운 MongoDB 연결이 생성되었습니다.")

        # 연결 상태 확인 및 재연결
        if not cls._is_connected():
            async with cls._lock:
                logger.warning("MongoDB 연결이 끊어졌습니다. 재연결을 시도합니다.")
                if cls._instance:
                    cls._instance.close()
                cls._instance = cls._create_client()
                logger.info("MongoDB 재연결이 완료되었습니다.")

        return cls._instance

    @classmethod
    def _create_client(cls) -> MongoClient:
        """MongoDB 클라이언트 생성"""
        return MongoClient(
            os.environ["mongodb_address"],
            maxPoolSize=10,  # 최대 연결 풀 크기
            minPoolSize=1,   # 최소 연결 풀 크기
            maxIdleTimeMS=30000,  # 30초 후 유휴 연결 정리
            serverSelectionTimeoutMS=5000,  # 5초 서버 선택 타임아웃
            connectTimeoutMS=10000,  # 10초 연결 타임아웃
        )

    @classmethod
    def _is_connected(cls) -> bool:
        """MongoDB 연결 상태 확인"""
        if cls._instance is None:
            return False

        try:
            # ping 명령으로 연결 상태 확인
            cls._instance.admin.command('ping')
            return True
        except Exception as e:
            logger.warning(f"MongoDB 연결 상태 확인 실패: {e}")
            return False

    @classmethod
    async def close(cls):
        """MongoDB 연결 정리"""
        if cls._instance is not None:
            async with cls._lock:
                if cls._instance is not None:
                    cls._instance.close()
                    cls._instance = None
                    logger.info("MongoDB 연결이 정리되었습니다.")

    @classmethod
    async def __aenter__(cls):
        return await cls.get_instance()

    @classmethod
    async def __aexit__(cls, exc_type, exc_val, exc_tb):
        # 컨텍스트 매니저로 사용할 때는 즉시 정리하지 않음
        # 다른 곳에서도 사용할 수 있기 때문
        pass
