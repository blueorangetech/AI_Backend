from pydantic import BaseModel

class TotalRequestModel(BaseModel):
    data : dict

class NaverRequestModel(BaseModel):
    customer_id : str
    table_name : str

class KakaoRequestModel(BaseModel):
    account_id: str
    table_name : str

class KakaoMomentRequestModel(BaseModel):
    account_id: str
    table_name : str

class GoogleAdsRequestModel(BaseModel):
    customer_id : str
    fields: str
    table_name : str

class GA4RequestModel(BaseModel):
    property_id : str
    table_name : str


class MetaAdsRequestModel(BaseModel):
    account_id: str
    table_name: str 
    start_date: str | None = None
    end_date: str | None = None