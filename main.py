from fastapi import FastAPI, UploadFile, File, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from analyst import analyst, media_analyst, keyword_analyst
from datetime import datetime, timedelta
from tools.pre_processing import date_group

import os, uuid, json, pandas, pytz

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/analysis/report")
async def data_analysis(file: UploadFile = File(...), standard: str = Form(), compare: str = Form()):

    repository = './file_repository'
    os.makedirs(repository, exist_ok=True)

    file_name = f"{str(uuid.uuid4())}.xlsx"
    file_path = os.path.join(repository, file_name)

    with open(file_path, "wb") as f:
        file_content = await file.read()
        f.write(file_content)

    pre_result = date_group(file_path, standard, compare)

    results = {}
    results["통합 리포트"] = analyst(pre_result)
    results["매체별 리포트"] = media_analyst(pre_result)
    
    if os.path.exists(file_path):
        os.remove(file_path)

    return results

@app.post("/analysis/keyword")
async def keyword_analysis(file: UploadFile = File(...), standard: str = Form(), compare: str = Form()):
    repository = './file_repository'
    os.makedirs(repository, exist_ok=True)

    file_name = f"{str(uuid.uuid4())}.xlsx"
    file_path = os.path.join(repository, file_name)

    with open(file_path, "wb") as f:
        file_content = await file.read()
        f.write(file_content)

    pre_result = date_group(file_path, standard, compare)
    result = keyword_analyst(pre_result)

    if os.path.exists(file_path):
        os.remove(file_path)

    return result