from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from analyst import analyst, media_analyst, keyword_analyst
import os, uuid

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
async def data_analysis(file: UploadFile = File(...)):
    repository = './file_repository'
    os.makedirs(repository, exist_ok=True)

    file_name = f"{str(uuid.uuid4())}.xlsx"
    file_path = os.path.join(repository, file_name)

    with open(file_path, "wb") as f:
        file_content = await file.read()
        f.write(file_content)

    results = {}
    results["통합 리포트"] = analyst(file_path)
    results["매체별 리포트"] = media_analyst(file_path)
    
    if os.path.exists(file_path):
        os.remove(file_path)

    return results

@app.post("/analysis/keyword")
async def keyword_analysis(file: UploadFile = File(...)):
    repository = './file_repository'
    os.makedirs(repository, exist_ok=True)

    file_name = f"{str(uuid.uuid4())}.xlsx"
    file_path = os.path.join(repository, file_name)

    with open(file_path, "wb") as f:
        file_content = await file.read()
        f.write(file_content)

    result = keyword_analyst(file_path)

    if os.path.exists(file_path):
        os.remove(file_path)

    return result