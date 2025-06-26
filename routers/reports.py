from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from services.naver_service import NaverReportService
from services.kakao_service import KakaoReportService
from services.google_service import GoogleAdsReportServices
from services.ga4_service import GA4ReportServices
from auth.kakao_token_manager import KakaoTokenManager
from models.total_request_models import TotalRequestModel
from models.naver_request_models import NaverRequestModel
from models.kakao_request_models import KakaoRequestModel
from models.google_request_models import GoogleAdsRequestModel, GA4RequestModel
from auth.naver_auth_manager import get_naver_client
from auth.google_auth_manager import get_google_ads_client, get_ga4_client

router = APIRouter(prefix="/reports", tags=["reports"])

@router.post("/all")
async def create_all_report(request: TotalRequestModel):
    try:
        result = {}
        for data in request.data:
            platform_data = request.data[data]
            if data.upper() == "NAVER":
                naver_request = NaverRequestModel(**platform_data)
                response = await create_naver_reports(naver_request)

            elif data.upper() == "KAKAO":
                kakao_request = KakaoRequestModel(**platform_data)
                response = await create_kakao_reports(kakao_request)
            
            elif data.upper() == "GOOGLE":
                google_request = GoogleAdsRequestModel(**platform_data)
                response = await create_google_report(google_request)
            
            elif data.upper() == "GA4":
                ga4_request = GA4RequestModel(**platform_data)
                response = await create_ga4_report(ga4_request)
            
            else:
                response = "지원하지 않는 매체입니다."
            
            result[data] = response

        for res in result:
            if not result[res]:
                return {"status": "some success", "message": result}
            
        return {"status": "success", "message": result}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/naver")
async def create_naver_reports(requset: NaverRequestModel):
    """ 네이버 광고 성과 다운로드 """
    try:
        client = get_naver_client(requset.customer_id)
        service = NaverReportService(client)

        response = await service.create_complete_report()
        # BigQuery 연결

        return True

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "customer_id": requset.customer_id
        }
    
@router.post("/kakao")
async def create_kakao_reports(request: KakaoRequestModel):
    """ 카카오 광고 성과 다운로드 """
    try:
        token_manager = KakaoTokenManager()
        vaild_token = await token_manager.get_vaild_token()

        service = KakaoReportService(vaild_token, request.account_id)
        result = await service.create_report()

        # BigQuery로 보내기

        return True
    
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
async def create_google_report(request: GoogleAdsRequestModel):
    """ 구글 광고 성과 다운로드 """
    try:
        client = get_google_ads_client(request.customer_id)
        service = GoogleAdsReportServices(client)
        response = service.create_keyword_reports()

        # BigQuery로 보내기
        return True
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/ga4/test")
async def create_ga4_report(request: GA4RequestModel):
    try:
        client = get_ga4_client(request.property_id)
        service = GA4ReportServices(client)

        response = service.properties_list()

        return True
    
    except Exception as e:
        return {"status": "error", "message": str(e)}