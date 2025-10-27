import httpx
import asyncio
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class HTTPClientManager:
    """
    공통 HTTP 클라이언트 매니저
    싱글톤 패턴으로 전역에서 하나의 httpx.AsyncClient 인스턴스를 재사용
    """
    _instance: Optional['HTTPClientManager'] = None
    _client: Optional[httpx.AsyncClient] = None
    _lock = asyncio.Lock()

    def __new__(cls) -> 'HTTPClientManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get_client(self) -> httpx.AsyncClient:
        """HTTP 클라이언트 인스턴스 반환"""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=httpx.Timeout(30.0),  # 30초 타임아웃
                        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
                    )
                    logger.info("새로운 HTTP 클라이언트가 생성되었습니다.")
        return self._client

    async def close(self):
        """HTTP 클라이언트 정리"""
        if self._client is not None:
            async with self._lock:
                if self._client is not None:
                    await self._client.aclose()
                    self._client = None
                    logger.info("HTTP 클라이언트가 정리되었습니다.")

    async def __aenter__(self):
        return await self.get_client()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # 컨텍스트 매니저로 사용할 때는 즉시 정리하지 않음
        # 다른 곳에서도 사용할 수 있기 때문
        pass


# 전역 인스턴스
http_client_manager = HTTPClientManager()


async def get_http_client() -> httpx.AsyncClient:
    """전역 HTTP 클라이언트 반환"""
    return await http_client_manager.get_client()


async def cleanup_http_client():
    """애플리케이션 종료 시 HTTP 클라이언트 정리"""
    await http_client_manager.close()