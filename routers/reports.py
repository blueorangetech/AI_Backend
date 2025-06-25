from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from services.naver_service import NaverReportService
from services.kakao_service import KakaoReportService
from services.google_service import GoogleReportServices
from auth.kakao_token_manager import KakaoTokenManager
from models.naver_request_models import NaverRequsetModel
from models.kakao_request_models import KakakoRequestModel
from models.google_request_models import GoogleRequsetModel
from auth.naver_auth_manager import get_naver_client
from auth.google_auth_manager import get_google_client

router = APIRouter(prefix="/reports", tags=["reports"])

@router.post("/naver")
async def create_naver_reports(requset: NaverRequsetModel):
    """ 네이버 광고 성과 다운로드 """
    try:
        client = get_naver_client(requset.customer_id)
        service = NaverReportService(client)

        response = await service.create_complete_report(requset.target_date)
        # BigQuery 연결

        return 

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "customer_id": requset.customer_id,
            "target_date": requset.target_date
        }
    
@router.post("/kakao")
async def create_kakao_reports(request: KakakoRequestModel):
    """ 카카오 광고 성과 다운로드 """
    try:
        token_manager = KakaoTokenManager()
        vaild_token = await token_manager.get_vaild_token()

        service = KakaoReportService(vaild_token, request.account_id)
        result = await service.create_report(request.start_date, request.end_date)

        # BigQuery로 보내기

        return result
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/kakao/token")
async def enroll_kakao_token(code: str = Query(...)):
    """ Kakao Token이 모두 만료 되면 수동으로 업데이트 진행 """
    try:
        token_manager = KakaoTokenManager()
        await token_manager.renewal_all_token(code)

        return JSONResponse(
            status_code = status.HTTP_201_CREATED,
            content={"message": "카카오 토큰이 성공적으로 등록되었습니다"}
        )
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/google/test")
async def create_google_report(request: GoogleRequsetModel):
    """ 구글 광고 성과 다운로드 """
    try:
        client = get_google_client(request.customer_id)
        service = GoogleReportServices(client)
        response = service.create_keyword_reports()

        # BigQuery로 보내기
        return
    
    except Exception as e:
        return {"status": "error", "message": str(e)}