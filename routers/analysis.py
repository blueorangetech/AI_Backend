from fastapi import APIRouter, Header, Form, UploadFile, HTTPException, File
from typing import Annotated, Union
from database.mongodb import MongoDB

from analyst import analyst, media_analyst, keyword_analyst
from tools.pre_processing import date_group

import json, jwt, os, uuid

jwt_token = os.environ["jwt_token_key"]

router = APIRouter(prefix="/analysis", tags=["analysis"])

mongodb = MongoDB.get_instance()
db = mongodb["Customers"]

repository = "./file_repository"
os.makedirs(repository, exist_ok=True)


@router.get("/cached")
async def get_cached_data(Authorization: Annotated[Union[str, None], Header()] = None):
    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        customer_name = payload["access"]

        collection = db["cache"]
        cahced_data = collection.find_one({"name": customer_name})

        if cahced_data is not None:
            del cahced_data["name"]
            del cahced_data["_id"]

            return cahced_data

        else:
            return None

    except:
        HTTPException(status_code=404, detail="캐시 데이터 호출 중 에러 발생")


@router.post("/report")
async def data_analysis(
    file: UploadFile = File(...),
    standard: str = Form(),
    compare: str = Form(),
    formula: str = Form(),
    depth: str = Form(),
    product: str = Form(),
    Authorization: Annotated[Union[str, None], Header()] = None,
):

    file_path = None

    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        customer_name = payload["access"]

        print(f"{customer_name} Request analysis")

        file_name = f"{str(uuid.uuid4())}.xlsx"
        file_path = os.path.join(repository, file_name)

        with open(file_path, "wb") as f:
            file_content = await file.read()
            f.write(file_content)

        fields, depth, product = (
            json.loads(formula),
            json.loads(depth),
            json.loads(product),
        )

        # 필드명 검증과 기간 분리
        pre_result, kst_standard, kst_compare = date_group(
            file_path, standard, compare, fields + depth
        )

        results = {}

        total_report = analyst(pre_result, kst_standard, kst_compare, fields)
        media_report = media_analyst(
            pre_result, kst_standard, kst_compare, fields, depth, product
        )

        results["통합 리포트"], results["매체별 리포트"] = total_report, media_report
        collection = db["cache"]

        stored_data = collection.find_one({"name": customer_name})

        if stored_data is None:
            data = {
                "name": customer_name,
                "통합 리포트": total_report,
                "매체별 리포트": media_report,
            }

            collection.insert_one(data)

        else:
            collection.update_one(
                {"name": customer_name},
                {"$set": {"통합 리포트": total_report, "매체별 리포트": media_report}},
            )

        return results

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


@router.post("/keyword")
async def keyword_analysis(
    file: UploadFile = File(...),
    standard: str = Form(),
    compare: str = Form(),
    keyword_formula: str = Form(),
    depth: str = Form(),
    Authorization: Annotated[Union[str, None], Header()] = None,
):

    file_path = None

    if Authorization is None:
        raise HTTPException(status_code=401, detail="Authorization 헤더가 필요합니다")

    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        customer_name = payload["access"]

        print(f"{customer_name} Request Keyword analysis")

        file_name = f"{str(uuid.uuid4())}.xlsx"
        file_path = os.path.join(repository, file_name)

        with open(file_path, "wb") as f:
            file_content = await file.read()
            f.write(file_content)

        fields, depth = json.loads(keyword_formula), json.loads(depth)

        # 필드명 검증과 기간 분리
        pre_result, kst_standard, kst_compare = date_group(
            file_path, standard, compare, fields + depth
        )

        result = keyword_analyst(pre_result, kst_standard, kst_compare, fields, depth)

        collection = db["cache"]
        stored_data = collection.find_one({"name": customer_name})

        if stored_data is None:
            data = {"name": customer_name}
            for key in result:
                data[key] = result[key]

            collection.insert_one(data)

        else:
            collection.update_one({"name": customer_name}, {"$set": result})

        return result

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
