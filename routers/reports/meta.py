from fastapi import APIRouter
from services.meta_service import MetaAdsReportServices
from auth.meta_auth_manager import get_meta_ads_client
from models.media_request_models import MediaRequestModel
from configs.customers_event import bo_customers

router = APIRouter(prefix="/meta", tags=["reports"])

@router.post("/test")
async def test(request: MediaRequestModel):
    try:
        customer = request.customer
        customer_info = bo_customers[customer]["media_list"]["meta"]
        data_set_name = bo_customers[customer]["data_set_name"]

        account_id = customer_info["account_id"]
        
        client = get_meta_ads_client(account_id)
        service = MetaAdsReportServices(client)

        # 임시 변수
        fields = customer_info["fields"]
        response = await service.create_reports(fields)
        return response
    
    except Exception as e:
        return {"status": "error", "message": str(e)}