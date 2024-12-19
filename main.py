from fastapi import FastAPI, UploadFile, File, Header, Form, HTTPException
from typing import Annotated, Union
from fastapi.middleware.cors import CORSMiddleware

from analyst import analyst, media_analyst, keyword_analyst
from tools.pre_processing import date_group
from models.customer_auth_check import Login_Info
from auth.auth_customer import authenticate
from tools.mongodb import MongoDB

import os, uuid, json, jwt, traceback

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

jwt_token = os.environ["jwt_token_key"]
mongodb = MongoDB.get_instance()
db = mongodb["Customers"]

@app.get("/")
async def hello():
    return "Welcome to BlueOrange Service !"

@app.get("/analysis/cached")
async def get_cached_data(Authorization: Annotated[Union[str, None], Header()] = None):
    payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
    customer_name = payload["access"]

    collection = db["cache"]
    cahced_data = collection.find_one({"name": customer_name})
    
    del cahced_data["name"]
    del cahced_data["_id"]

    return cahced_data


@app.post("/analysis/report")
async def data_analysis(file: UploadFile = File(...), standard: str = Form(), 
                        compare: str = Form(), formula: str = Form(), depth: str = Form(),
                        Authorization: Annotated[Union[str, None], Header()] = None):
    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        customer_name = payload["access"]

        print(f"{customer_name} Request analysis")

        repository = './file_repository'
        os.makedirs(repository, exist_ok=True)

        file_name = f"{str(uuid.uuid4())}.xlsx"
        file_path = os.path.join(repository, file_name)

        with open(file_path, "wb") as f:
            file_content = await file.read()
            f.write(file_content)

        fields, depth = json.loads(formula), json.loads(depth)
        pre_result, kst_standard, kst_compare = date_group(file_path, standard, compare)
        
        results = {}

        total_report = analyst(pre_result, kst_standard, kst_compare, fields)
        media_report = media_analyst(pre_result, kst_standard, kst_compare, fields, depth)
        
        results["통합 리포트"], results["매체별 리포트"] = total_report, media_report
        collection = db["cache"]

        stored_data = collection.find_one({"name": customer_name})

        if stored_data is None:
            data = {"name": customer_name, 
                    "통합 리포트": total_report, "매체별 리포트" : media_report}
            
            collection.insert_one(data)

        else:
            collection.update_one({"name": customer_name}, 
                                  {"$set": {"통합 리포트": total_report,
                                            "매체별 리포트" : media_report}
                                            })

        return results
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@app.post("/analysis/keyword")
async def keyword_analysis(file: UploadFile = File(...), standard: str = Form(), 
                           compare: str = Form(), keyword_formula: str = Form(), 
                           depth: str = Form(),
                           Authorization: Annotated[Union[str, None], Header()] = None):
    try:
        payload = jwt.decode(Authorization, jwt_token, algorithms="HS256")
        customer_name = payload["access"]

        print(f"{customer_name} Request Keyword analysis")

        repository = './file_repository'
        os.makedirs(repository, exist_ok=True)

        file_name = f"{str(uuid.uuid4())}.xlsx"
        file_path = os.path.join(repository, file_name)

        with open(file_path, "wb") as f:
            file_content = await file.read()
            f.write(file_content)

        pre_result, kst_standard, kst_compare = date_group(file_path, standard, compare)
        
        fields, depth = json.loads(keyword_formula), json.loads(depth)
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
        print(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as exc:
        print(exc)
        raise HTTPException(status_code=404, detail=str(exc))
    
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


@app.post("/auth/{customer_url}")
async def login(login_info : Login_Info):
    try:
        name, password = login_info.name, login_info.password
        if authenticate(name, password):
            payload = {"access": name}
            token = jwt.encode(payload, jwt_token, algorithm="HS256")
            return  {"status": True, "token": token}

        else:
            raise HTTPException(status_code=404, detail="비밀번호를 확인하세요")
    
    except HTTPException as http_exc:
        raise http_exc

    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=404, detail="시스템 에러 발생")

@app.get("/auth/{customer_url}")
async def check_header(customer_url: str, 
                       Authorization: Annotated[Union[str, None], Header()] = None):
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