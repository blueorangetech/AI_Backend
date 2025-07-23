from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import analysis, auth, mix_models, reports
import logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analysis.router)
app.include_router(auth.router)
app.include_router(mix_models.router)
app.include_router(reports.router)

@app.get("/")
async def hello():
    return "Welcome to BlueOrange Service !"