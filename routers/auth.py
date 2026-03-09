from fastapi import APIRouter, HTTPException, Header, Body
from typing import Annotated, Union, Optional

from models.customer_auth_check import Login_Info, Register_Info
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
    """ 현재 접속하려는 업체 URL에 대한 권한이 있는지 검증 """
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
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다.")
    except Exception:
        raise HTTPException(status_code=404, detail="시스템 에러 발생")


@router.post("/register")
async def register(register_info: Register_Info):
    """ 회원가입 앤드포인트 """
    try:
        user_id = register_info.user_id
        name = register_info.name
        password = register_info.password
        
        login_manager = LoginAuthManger()
        
        success = await login_manager.store_customer(user_id, name, password)
        
        if success:
            return {"status": "success", "message": f"{name}({user_id}) 님, 회원가입을 축하합니다!"}
        else:
            raise HTTPException(status_code=400, detail="이미 존재하는 아이디입니다.")

    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="회원가입 중 서버 에러가 발생했습니다.")


@router.post("/login")
async def login(login_info: Login_Info):
    """ 공통 로그인 앤드포인트 """
    try:
        user_id, password = login_info.user_id, login_info.password
        login_manager = LoginAuthManger()

        auth_result = await login_manager.authenticate(user_id, password)
        if auth_result["status"]:
            customer_data = auth_result["customer"]
            access_list = customer_data.get("access_list", {})
            is_master = customer_data.get("is_master", False)
            username = customer_data.get("name")

            payload = {
                "user_id": user_id,
                "name": username,
                "access_list": access_list,
                "is_master": is_master
            }
            token = jwt.encode(payload, jwt_token, algorithm="HS256")
            return {
                "status": "success", 
                "token": token, 
                "name": username,
                "is_master": is_master,
                "access_list": access_list
            }

        else:
            raise HTTPException(status_code=401, detail=auth_result["message"])

    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")

# --- 관리 권한 전용 API (승격 로직 최적화) ---

@router.get("/{customer_url}/manage/users")
async def get_users(
    customer_url: str,
    Authorization: Annotated[Union[str, None], Header()] = None
):
    """ 유저 정보 조회 (DB 기반 권한 검증) """
    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        requester_id = payload.get("user_id")

        login_manager = LoginAuthManger()
        
        requester_info = await login_manager.get_user_by_id(requester_id)
        if not requester_info:
            raise HTTPException(status_code=401, detail="유효하지 않은 계정입니다.")
        
        is_master_requester = requester_info.get("is_master", False)
        access_list_requester = requester_info.get("access_list", {})
        
        role = access_list_requester.get(customer_url)
        if not is_master_requester and role != "admin":
            raise HTTPException(status_code=403, detail="해당 업체의 유저 목록을 볼 권한이 없습니다.")

        users = await login_manager.get_all_customers()
        return {"status": "success", "users": users}

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다.")
    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")


@router.patch("/{customer_url}/manage/{target_id}")
async def manage_user(
    customer_url: str,
    target_id: str,
    Authorization: Annotated[Union[str, None], Header()] = None,
    role: Optional[str] = Body(None),
    is_master: Optional[bool] = Body(None)
):
    """ 유저 권한 관리 (보호 로직 강화) """
    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        requester_id = payload.get("user_id")

        login_manager = LoginAuthManger()
        requester_info = await login_manager.get_user_by_id(requester_id)
        if not requester_info:
            raise HTTPException(status_code=401, detail="유효하지 않은 계정입니다.")

        is_master_requester = requester_info.get("is_master", False)
        my_access_list = requester_info.get("access_list", {})
        my_role = my_access_list.get(customer_url)

        # 대상 유저 정보 조회
        target_user = await login_manager.get_user_by_id(target_id)
        if not target_user:
            raise HTTPException(status_code=404, detail="대상 유저를 찾을 수 없습니다.")
        
        # [핵심 보호] 대상 유저가 Master인 경우, 요청자도 Master여야만 수정 가능
        if target_user.get("is_master") and not is_master_requester:
             raise HTTPException(status_code=403, detail="Master 유저의 권한은 Admin이 수정할 수 없습니다.")

        current_access_list = target_user.get("access_list", {})
        final_is_master = None
        final_access_list = None

        if is_master_requester:
            # Master는 모든 기능을 자유롭게 수행
            if is_master is not None:
                final_is_master = is_master
            if role is not None:
                if role in ["none", ""]:
                    current_access_list.pop(customer_url, None)
                else:
                    current_access_list[customer_url] = role
                final_access_list = current_access_list
        
        elif my_role == "admin":
            # [수정] Admin은 Master '권한 부여(True)'만 금지함
            if is_master is True:
                raise HTTPException(status_code=403, detail="Admin은 Master 권한을 부여할 수 없습니다.")
            
            if role is not None:
                if role in ["none", ""]:
                    current_access_list.pop(customer_url, None)
                elif role == "viewer":
                    current_access_list[customer_url] = "viewer"
                else:
                    # Admin은 admin 권한을 줄 수 없음
                    raise HTTPException(status_code=403, detail="Admin은 viewer 역할만 부여할 수 있습니다.")
                final_access_list = current_access_list
        else:
            raise HTTPException(status_code=403, detail="권한 수정 권한이 없습니다.")

        # 업데이트 수행
        success = await login_manager.update_customer_privilege(
            target_id, 
            access_list=final_access_list, 
            is_master=final_is_master
        )

        if success:
            return {"status": "success", "message": "유저 권한이 성공적으로 변경되었습니다."}
        else:
            return {"status": "no_change", "message": "변경 사항이 없습니다."}

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="토큰이 만료되었습니다.")
    except HTTPException as http_exc:
        raise http_exc
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")