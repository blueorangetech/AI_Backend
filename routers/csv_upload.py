from fastapi import APIRouter, UploadFile, File, Form, status, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from services.csv_service import CSVService
from auth.google_auth_manager import get_bigquery_client, get_gcs_client
from models.bigquery_schemas import imweb_inner_data_schema, hanssem_insight_schema
from services.data_processor import DataProcessor
import logging
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

@router.post("/upload/request-upload-url")
async def request_upload_url(filename: str = Form(...)):
    """
    대용량 CSV 파일 업로드를 위한 GCS Signed URL 생성

    클라이언트가 이 URL로 직접 GCS에 파일을 업로드할 수 있습니다.
    Cloud Run의 32MB 제한을 우회하여 대용량 파일 업로드 가능.

    Args:
        filename: 업로드할 파일명

    Returns:
        {
            "upload_url": "GCS Signed URL",
            "blob_name": "GCS blob 경로",
            "expiration_minutes": 30
        }
    """
    DATASET_ID = "imweb"
    TABLE_ID = "INNER_data"

    try:
        logger.info(f"Signed URL 요청: {filename}")

        # GCS 클라이언트 초기화
        gcs_client = get_gcs_client(GCS_BUCKET_NAME)

        # Blob 이름 생성
        blob_name = gcs_client.generate_blob_name(DATASET_ID, TABLE_ID, filename)

        # Signed URL 생성
        upload_url = await gcs_client.generate_signed_url(
            blob_name=blob_name,
            expiration_minutes=30,
            method="PUT"
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "upload_url": upload_url,
                "blob_name": blob_name,
                "expiration_minutes": 30
            }
        )

    except Exception as e:
        logger.error(f"Signed URL 생성 실패: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Signed URL 생성 실패: {str(e)}"
        )


@router.post("/upload/process-uploaded-file")
async def process_uploaded_file(blob_name: str = Form(...)):
    """
    GCS에 업로드된 CSV 파일을 처리하여 BigQuery에 로드

    클라이언트가 GCS에 직접 업로드한 후 이 엔드포인트를 호출하면
    서버가 파일을 읽어서 이상치 제거 후 BigQuery에 로드합니다.

    Args:
        blob_name: GCS blob 경로

    Returns:
        업로드 결과
    """
    DATASET_ID = "imweb"
    TABLE_ID = "INNER_data"

    try:
        logger.info(f"GCS 업로드된 파일 처리 시작: {blob_name}")

        # BigQuery 및 GCS 클라이언트 초기화
        bigquery_client = get_bigquery_client()
        gcs_client = get_gcs_client(GCS_BUCKET_NAME)
        csv_service = CSVService(bigquery_client, gcs_client)

        # IMWEB 스키마 가져오기
        schema = imweb_inner_data_schema()

        # GCS에서 파일 처리
        result = await csv_service.gcs_file_to_bigquery(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            blob_name=blob_name,
            schema=schema,
            truncate=True,
            processor_func=DataProcessor.process_imweb_inner_data
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )

    except Exception as e:
        logger.error(f"GCS 파일 처리 실패: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"GCS 파일 처리 실패: {str(e)}"}
        )

@router.post("/upload/direct")
async def upload_file_direct(
    file: UploadFile = File(...),
    dataset_id: str = Form(...),
    table_id: str = Form(...),
    truncate: bool = Form(True)
):
    """
    클라이언트로부터 파일을 직접 받아 BigQuery에 저장 (GCS 경유 안 함)
    CSV, XLSX 지원
    """
    try:
        logger.info(f"직접 업로드 요청 시작: {file.filename} (Target: {dataset_id}.{table_id})")
        
        # 파일 내용 읽기
        content = await file.read()
        
        # 서비스 초기화
        bigquery_client = get_bigquery_client()
        csv_service = CSVService(bigquery_client)

        # 스키마 불러오기
        hanssem_schema = hanssem_insight_schema()
        
        # 직접 업로드 실행
        result = await csv_service.upload_file_direct(
            dataset_id=dataset_id,
            table_id=table_id,
            file_content=content,
            filename=file.filename or "unknown_file",
            schema=hanssem_schema,
            truncate=truncate,
            processor_func=DataProcessor.process_hanssem_report
        )
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )

    except Exception as e:
        logger.error(f"직접 업로드 실패: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"업로드 처리 실패: {str(e)}"
        )
