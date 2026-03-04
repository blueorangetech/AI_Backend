from fastapi import APIRouter, HTTPException, Header, Body
from typing import Annotated, Union, Optional

from models.customer_auth_check import Login_Info
from auth.auth_customer import LoginAuthManger
import logging
import os, jwt, traceback

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])
jwt_token = os.environ["jwt_token_key"]

@router.get("/{customer_url}")
async def check_header(
    customer_url: str, Authorization: Annotated[Union[str, None], Header()] = None
):

    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        
        access_list = payload.get("access_list", {})
        is_master = payload.get("is_master", False)
        
        login_manager = LoginAuthManger()
        auth_check = await login_manager.check_authority(customer_url, access_list, is_master)
        
        if not auth_check["status"]:
            raise HTTPException(status_code=403, detail=auth_check["message"])

        return {"status": True, "role": auth_check["role"]}

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        raise HTTPException(status_code=404, detail="시스템 에러 발생")


@router.post("/{customer_url}")
async def login(customer_url: str, login_info: Login_Info):
    try:
        name, password = login_info.name, login_info.password
        login_manager = LoginAuthManger()

        auth_result = await login_manager.authenticate(name, password)
        if auth_result["status"]:
            customer_data = auth_result["customer"]
            access_list = customer_data.get("access_list", {})
            is_master = customer_data.get("is_master", False)
            
            auth_check = await login_manager.check_authority(customer_url, access_list, is_master)
            
            if not auth_check["status"]:
                raise HTTPException(status_code=403, detail=auth_check["message"])

            payload = {
                "access": name,
                "access_list": access_list,
                "is_master": is_master
            }
            token = jwt.encode(payload, jwt_token, algorithm="HS256")
            return {"status": "success", "token": token, "role": auth_check["role"]}

        else:
            raise HTTPException(status_code=401, detail=auth_result["message"])

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")

# --- 관리 권한 전용 API ---

@router.patch("/manage/{target_name}")
async def manage_user(
    target_name: str,
    Authorization: Annotated[Union[str, None], Header()] = None,
    is_master: Optional[bool] = Body(None),
    access_list: Optional[dict] = Body(None)
):
    """
    유저 승격 및 권한 관리 (Master 전용)
    - is_master: True/False 로 마스터 권한 부여/취소
    - access_list: 해당 유저의 접근 가능 업체 리스트(dict) 업데이트
    """
    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        # 1. 요청자가 마스터인지 확인
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        if not payload.get("is_master"):
            raise HTTPException(status_code=403, detail="마스터 권한이 필요합니다.")

        # 2. 유저 정보 업데이트
        login_manager = LoginAuthManger()
        success = await login_manager.update_customer_privilege(
            target_name, 
            access_list=access_list, 
            is_master=is_master
        )

        if success:
            return {"status": "success", "message": f"{target_name} 유저의 권한이 업데이트되었습니다."}
        else:
            raise HTTPException(status_code=400, detail="업데이트에 실패했습니다. 유저 아이디를 확인하거나 변경 사항이 없습니다.")

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다.")
        
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")