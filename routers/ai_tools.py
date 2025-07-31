from fastapi import APIRouter
from pydantic import BaseModel
import httpx

router = APIRouter(prefix="/aitools", tags=["aitools"])

class RequestModel(BaseModel):
    url: str

@router.post("/search")
async def search_internet(request: RequestModel):
    async with httpx.AsyncClient() as client:
        response = await client.get(request.url)
        return {
            "status_code": response.status_code,
            "content": response.text,
            "headers": dict(response.headers)
        }