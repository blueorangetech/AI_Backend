from pydantic import BaseModel

class KakaoRequestModel(BaseModel):
    account_id: str