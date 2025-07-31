from fastapi import APIRouter
from pydantic import BaseModel
import httpx, logging

router = APIRouter(prefix="/aitools", tags=["aitools"])

logger = logging.getLogger(__name__)

class RequestModel(BaseModel):
    url: str

@router.post("/search")
async def search_internet(request: RequestModel):
    async with httpx.AsyncClient() as client:
        response = await client.get(request.url)
        logger.info(f"GPT use fetchHtml Tool")
        result = response.text
        return {"html": result}