from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import reports, token, csv_upload
from utils.http_client_manager import cleanup_http_client
from utils.bigquery_client_manager import cleanup_all_bigquery_clients
from database.mongodb import MongoDB
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 애플리케이션 시작 시
    yield
    # 애플리케이션 종료 시 리소스 정리
    await cleanup_http_client()
    await cleanup_all_bigquery_clients()
    await MongoDB.close()

app = FastAPI(lifespan=lifespan)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(reports.router)
app.include_router(token.router)
app.include_router(csv_upload.router)

@app.get("/")
async def hello():
    return "Welcome to BlueOrange Service !"
