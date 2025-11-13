from fastapi import APIRouter, UploadFile, File, Form, status, HTTPException
from fastapi.responses import JSONResponse
from services.csv_service import CSVService
from auth.google_auth_manager import get_bigquery_client, get_gcs_client
from models.bigquery_schemas import imweb_inner_data_schema
from typing import Optional
import logging
import json
import os

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/csv", tags=["csv"])

# GCS 버킷 이름 (환경 변수 또는 설정에서 가져오기)
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "blorange")

@router.post("/upload/imweb")
async def upload_imweb_csv(
    file: UploadFile = File(..., description="업로드할 IMWEB CSV 파일")):
    """
    IMWEB INNER_data 테이블 전용 CSV 업로드 엔드포인트

    **큰 CSV 파일에 최적화**: 스트리밍 방식으로 GCS를 통해 BigQuery에 로드합니다.
    메모리에 전체 파일을 로드하지 않아 대용량 파일 처리가 가능합니다.

    - **file**: CSV 파일 (대용량 지원)
    - **truncate**: True이면 기존 테이블 데이터를 모두 삭제 후 삽입 (기본: True)

    고정값:
    - dataset_id: "imweb"
    - table_id: "INNER_data"
    - 필수 컬럼: 17개
    - 스키마: imweb_inner_data_schema (자동 적용)
    - GCS 버킷: run-sources-my-project-carrot-407906-asia-northeast3

    처리 과정:
    1. CSV 파일을 GCS에 스트리밍 업로드
    2. BigQuery가 GCS에서 직접 데이터 로드 (대용량 최적화)
    3. 업로드 완료 후 GCS 임시 파일 자동 삭제
    """
    DATASET_ID = "imweb"
    TABLE_ID = "INNER_data"
    REQUIRED_COLUMNS = [
        "site_owner_member",
        "site_code",
        "site_creator_member",
        "main_domain",
        "site_owner_join_time",
        "site_creation_time",
        "first_plan_payment_time",
        "first_plan_version",
        "first_plan_period",
        "first_plan_price_with_tax",
        "plan_end_time",
        "trial_start_time",
        "trial_version",
        "is_subscription_active",
        "subscription_type",
        "is_pg_or_pay_active",
        "site_payment_lead_time",
        "first_payment_operation_type"
    ]

    try:
        # 파일 확장자 확인
        if not file.filename or not file.filename.endswith('.csv'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV 파일만 업로드 가능합니다"
            )

        logger.info(f"IMWEB CSV 파일 업로드 시작 (GCS 스트리밍): {file.filename} → {DATASET_ID}.{TABLE_ID}")

        # BigQuery 및 GCS 클라이언트 초기화
        bigquery_client = get_bigquery_client()
        gcs_client = get_gcs_client(GCS_BUCKET_NAME)
        csv_service = CSVService(bigquery_client, gcs_client)

        # IMWEB 스키마 가져오기
        schema = imweb_inner_data_schema()

        # CSV 스트리밍 업로드 처리 (메모리 효율적)
        result = await csv_service.upload_csv_stream_via_gcs_to_bigquery(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            file_obj=file.file,  # 파일 객체 직접 전달 (메모리에 로드하지 않음)
            filename=file.filename,
            schema=schema,
            truncate=True,
            validate_columns=False  # 큰 파일에서는 검증 스킵
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"IMWEB CSV 업로드 중 오류 발생: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"IMWEB CSV 업로드 실패: {str(e)}"
        )
