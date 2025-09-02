from fastapi import APIRouter, HTTPException, Header
from typing import Annotated, Union

from models.customer_auth_check import Login_Info
from auth.auth_customer import authenticate

import os, jwt, traceback

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
        if customer_url != payload["access"]:
            raise HTTPException(status_code=404, detail="접근 권한이 없습니다")

        else:
            return {"status": True}

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        raise HTTPException(status_code=404, detail="시스템 에러 발생")


@router.post("/{customer_url}")
async def login(login_info: Login_Info):
    try:
        name, password = login_info.name, login_info.password
        if authenticate(name, password):
            payload = {"access": name}
            token = jwt.encode(payload, jwt_token, algorithm="HS256")
            return {"status": True, "token": token}

        else:
            raise HTTPException(status_code=404, detail="비밀번호를 확인하세요")

    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")
