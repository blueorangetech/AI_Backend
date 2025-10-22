from fastapi import APIRouter

from models.media_request_models import TotalRequestModel, MediaRequestModel

# 각 매체별 엔드포인트 함수 import
from routers.reports.naver import create_naver_reports, create_gfa_reports
from routers.reports.kakao import create_kakao_reports, create_kakao_monent_reports
from routers.reports.google import create_google_report, create_ga4_report
from routers.reports.meta import create_meta_reports

# 서브 라우터 포함 (매체별 엔드포인트)
from .naver import router as naver_router
from .kakao import router as kakao_router
from .google import router as google_router
from .meta import router as meta_router

from configs.customers_event import bo_customers

router = APIRouter(prefix="/reports", tags=["reports"])

# 서브 라우터들을 포함
router.include_router(naver_router)
router.include_router(kakao_router)
router.include_router(google_router)
router.include_router(meta_router)


@router.post("/all")
async def create_all_report(request: TotalRequestModel):
    try:
        media_config = {
            "naver": {
                "model_class": MediaRequestModel,
                "handler": create_naver_reports,
            },
            "gfa": {
                "model_class": MediaRequestModel,
                "handler": create_gfa_reports,
            },
            "kakao": {
                "model_class": MediaRequestModel,
                "handler": create_kakao_reports,
            },
            "kakao_moment": {
                "model_class": MediaRequestModel,
                "handler": create_kakao_monent_reports,
            },
            "google_ads": {
                "model_class": MediaRequestModel,
                "handler": create_google_report,
            },
            "ga4": {"model_class": MediaRequestModel, 
                    "handler": create_ga4_report
            },
            "meta": {
                "model_class": MediaRequestModel,
                "handler": "create_meta_reports"
            },  
        }
        result = {}

        for customer in request.customers:
            result[customer] = {}

            for media in bo_customers[customer]["media_list"].keys():    
                if media in media_config:
                    platform_data = {"customer": customer}
                    # 매체별 처리
                    config = media_config[media]
                    request_model = config["model_class"](**platform_data)
                    response = await config["handler"](request_model)

                else:
                    response = "지원하지 않는 매체입니다."

                result[customer][media] = response

        return {"status": "success", "message": result}

    except Exception as e:
        return {"status": "error", "message": str(e)}