from pydantic import BaseModel

class GoogleAdsRequestModel(BaseModel):
    customer_id : str

class GA4RequestModel(BaseModel):
    property_id : str