from fastapi import APIRouter, UploadFile, File, Form, status, HTTPException
from fastapi.responses import JSONResponse
from services.csv_service import CSVService
from auth.google_auth_manager import get_bigquery_client
from models.bigquery_schemas import imweb_inner_data_schema
from typing import Optional
import logging
import json

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/csv", tags=["csv"])

@router.post("/upload/imweb")
async def upload_imweb_csv(
    file: UploadFile = File(..., description="업로드할 IMWEB CSV 파일"),
    truncate: bool = Form(True, description="기존 데이터 삭제 여부 (기본: True)")
):
    """
    IMWEB INNER_data 테이블 전용 CSV 업로드 엔드포인트

    - **file**: CSV 파일
    - **truncate**: True이면 기존 테이블 데이터를 모두 삭제 후 삽입 (기본: True)

    고정값:
    - dataset_id: "imweb"
    - table_id: "INNER_data"
    - 필수 컬럼: 17개 (자동 체크)
    - 스키마: imweb_inner_data_schema (자동 적용)
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

        # 파일 읽기
        file_content = await file.read()

        logger.info(f"IMWEB CSV 파일 업로드 시작: {file.filename} → {DATASET_ID}.{TABLE_ID}")

        # BigQuery 클라이언트 및 CSV 서비스 초기화
        bigquery_client = get_bigquery_client()
        csv_service = CSVService(bigquery_client)

        # IMWEB 스키마 가져오기
        schema = imweb_inner_data_schema()

        # CSV 업로드 처리
        result = await csv_service.upload_csv_to_bigquery(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            file_content=file_content,
            schema=schema,
            required_columns=REQUIRED_COLUMNS,
            truncate=truncate
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
