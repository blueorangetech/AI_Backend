from fastapi import APIRouter
from pydantic import BaseModel
from bs4 import BeautifulSoup
from configs.customers_event import bo_customers
from services.bigquery_service import BigQueryReportService
from auth.google_auth_manager import get_bigquery_client
import httpx, logging, os

router = APIRouter(prefix="/tools", tags=["tools"])

logger = logging.getLogger(__name__)


class RequestModel(BaseModel):
    url: str


class TrendRequestModel(BaseModel):
    start_date: str
    end_date: str
    time_unit: str
    keyword_groups: list

class DmpModel(BaseModel):
    customer: str
    data: dict


@router.post("/search")
async def search_internet(request: RequestModel):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            request.url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
        )

        soup = BeautifulSoup(response.text, "html.parser")

        # 제목 추출
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""

        meta_og_title_tag = soup.find("meta", attrs={"property": "og:title"})
        meta_og_title = meta_og_title_tag.get("content", "").strip() if meta_og_title_tag else ""  # type: ignore

        # 메타 설명 추출
        meta_desc_tag = soup.find("meta", attrs={"name": "description"})
        meta_description = meta_desc_tag.get("content", "").strip() if meta_desc_tag else ""  # type: ignore

        meta_og_desc_tag = soup.find("meta", attrs={"property": "og:description"})
        meta_og_desc = meta_og_desc_tag.get("content", "").strip() if meta_og_desc_tag else ""  # type: ignore

        # H1 태그들 추출
        h1_tags = [h1.get_text(strip=True) for h1 in soup.find_all("h1")]

        # P 태그 추출
        p_tags = [p.get_text(strip=True) for p in soup.find_all("p")]

        logger.info(f"GPT use fetchHtml Tool")

        result = {
            "title": title,
            "meta_og_title": meta_og_title,
            "meta_description": meta_description,
            "meta_og_desc": meta_og_desc,
            "h1_tags": h1_tags,
            "p_tags": p_tags,
        }

        return result


@router.post("/trend")
async def get_trend(request: TrendRequestModel):
    url = "https://openapi.naver.com/v1/datalab/search"

    headers = {
        "X-Naver-Client-Id": os.environ["NAVER_CLIENT_ID"],
        "X-Naver-Client-Secret": os.environ["NAVER_CLIENT_SECRET"],
        "Content-Type": "application/json",
    }

    body = {
        "startDate": request.start_date,
        "endDate": request.end_date,
        "timeUnit": request.time_unit,
        "keywordGroups": request.keyword_groups,
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=body)

    if response.status_code == 200:
        return response.json()

    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
@router.post("/dmp")
async def upload_dmp(request: DmpModel):
    customer = request.customer
    data_set_name = bo_customers[customer]["data_set_name"]
    table_name = bo_customers[customer]["dmp_table"]

    dmp_data = {table_name: [request.data]}

    bigquery_client = get_bigquery_client()
    bigquery_service = BigQueryReportService(bigquery_client)

    result = await bigquery_service.insert_daynamic_schema_without_date(data_set_name, dmp_data)
    return result
