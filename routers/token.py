from fastapi import APIRouter, status, Query
from fastapi.responses import JSONResponse
from models.media_request_models import GFATokenRequestModel
from auth.kakao_token_manager import KakaoTokenManager
from auth.gfa_token_manager import GFATokenManager
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/token", tags=["token"])

@router.get("/kakao")
async def enroll_kakao_token(code: str = Query(...)):
    """Kakao Token이 모두 만료 되면 수동으로 업데이트 진행"""
    try:
        token_manager = KakaoTokenManager()
        await token_manager.renewal_all_token(code)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"message": "카카오 토큰이 성공적으로 등록되었습니다"},
        )

    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/gfa")
async def enroll_gfa_token(request: GFATokenRequestModel):
    """GFA Token이 모두 만료 되면 수동으로 업데이트 진행"""
    code = request.code
    received_state = request.state

    try:
        token_manager = GFATokenManager()
        await token_manager.renewal_all_token(code, received_state)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"message": "GFA 토큰이 성공적으로 등록되었습니다"},
        )

    except Exception as e:
        return {"status": "error", "message": str(e)}





