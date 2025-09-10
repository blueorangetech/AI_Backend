from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth, reports, ai_tools
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

app.include_router(auth.router)
app.include_router(reports.router)
app.include_router(ai_tools.router)


@app.get("/")
async def hello():
    return "Welcome to BlueOrange Service !"
