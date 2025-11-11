from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class CSVUploadResponse(BaseModel):
    """CSV 업로드 응답 모델"""
    status: str
    message: str
    dataset_id: str
    table_id: str
    rows_inserted: int
    truncated: bool


class CSVPreviewResponse(BaseModel):
    """CSV 미리보기 응답 모델"""
    status: str
    filename: str
    row_count: int
    column_count: int
    columns: List[str]
    dtypes: Dict[str, str]
    preview: List[Dict[str, Any]]


class CSVInfoModel(BaseModel):
    """CSV 파일 정보 모델"""
    row_count: int
    column_count: int
    columns: List[str]
    dtypes: Dict[str, str]
    preview: Optional[List[Dict[str, Any]]] = None
