from pydantic import BaseModel

class TotalRequestModel(BaseModel):
    data : dict

class NaverRequestModel(BaseModel):
    customer_id : str

class KakaoRequestModel(BaseModel):
    account_id: str

class GoogleAdsRequestModel(BaseModel):
    customer_id : str

class GA4RequestModel(BaseModel):
    property_id : str