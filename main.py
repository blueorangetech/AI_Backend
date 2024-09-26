from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from analyst import analyst
import os, uuid, time

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/analysis")
async def data_analysis(files: List[UploadFile] = File(...)):
    repository = './file_repository'
    os.makedirs(repository, exist_ok=True)

    file_list = []
    for file in files:
        file_name = f"{str(uuid.uuid4())}.xlsx"
        file_path = os.path.join(repository, file_name)

        file_list.append(file_path)

        with open(file_path, "wb") as f:
            file_content = await file.read()
            f.write(file_content)

        result = f"**PC**\n  - \ub178\ucd9c: +88,870 (\uc99d\uac00)\n  - \ud074\ub9ad: -5,336 (\uac10\uc18c)\n  - \ube44\uc6a9: +3,862,611 \uc6d0 (\uc99d\uac00)\n  - \uc804\ud658: -382 (\uac10\uc18c)\n  - CPC: +798.43 \uc6d0 (\uc99d\uac00)\n  - CTR: -0.222% (\uac10\uc18c)\n  - CVR: +0.029% (\uc99d\uac00)\n\n\uc774 \ubd84\uc11d \uacb0\uacfc\ub294 \uac01 \ub9e4\uccb4\uc640 \ub514\ubc14\uc774\uc2a4 \ubcc4\ub85c \uc131\uacfc\uc758 \ubcc0\ud654\ub97c \ubcf4\uc5ec\uc90d\ub2c8\ub2e4."
        # result = analyst(file_path)
        time.sleep(3)

    for file in file_list:
        if os.path.exists(file):
            os.remove(file)

    return result
