from fastapi import APIRouter, UploadFile, File, Form, status, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from services.csv_service import CSVService
from auth.google_auth_manager import get_bigquery_client, get_gcs_client
from models.bigquery_schemas import imweb_inner_data_schema
from typing import Optional
import logging
import json
import os

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/csv", tags=["csv"])

# CORS preflight 요청 디버깅용
@router.options("/upload/imweb")
async def options_imweb():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        }
    )

# GCS 버킷 이름 (환경 변수 또는 설정에서 가져오기)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "blorange")

@router.post("/upload/imweb")
async def upload_imweb_csv(
    file: UploadFile = File(..., description="업로드할 IMWEB CSV 파일")):
    """
    IMWEB INNER_data 테이블 전용 CSV 업로드 엔드포인트 (이상치 제거 기능 포함)

    **이상치 제거 프로세스**:
    1. CSV 파일을 GCS에 스트리밍 업로드
    2. GCS에서 파일을 다운로드하여 pandas DataFrame으로 변환
    3. 사용자 정의 이상치 제거 로직 적용 (csv_service.remove_outliers)
    4. GCS 임시 파일 삭제
    5. 정제된 데이터를 BigQuery에 직접 삽입

    고정값:
    - dataset_id: "imweb"
    - table_id: "INNER_data"
    - 스키마: imweb_inner_data_schema (자동 적용)

    **이상치 제거 로직 구현 방법**:
    services/csv_service.py의 remove_outliers 메서드에서 이상치 제거 로직을 구현하세요.
    """
    DATASET_ID = "imweb"
    TABLE_ID = "INNER_data"

    try:
        # 파일 확장자 확인
        if not file.filename or not file.filename.endswith('.csv'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV 파일만 업로드 가능합니다"
            )

        logger.info(f"IMWEB CSV 파일 업로드 시작 (이상치 제거): {file.filename} → {DATASET_ID}.{TABLE_ID}")

        # BigQuery 및 GCS 클라이언트 초기화
        bigquery_client = get_bigquery_client()
        gcs_client = get_gcs_client(GCS_BUCKET_NAME)
        csv_service = CSVService(bigquery_client, gcs_client)

        # IMWEB 스키마 가져오기
        schema = imweb_inner_data_schema()

        # 이상치 제거 후 CSV 업로드
        result = await csv_service.upload_csv_stream_with_outlier_removal(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            file_obj=file.file,
            filename=file.filename,
            schema=schema,
            truncate=True
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"IMWEB CSV 업로드 (이상치 제거) 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"IMWEB CSV 업로드 (이상치 제거) 실패: {str(e)}"
        )
