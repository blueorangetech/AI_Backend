from pydantic import BaseModel

class KakakoRequestModel(BaseModel):
    account_id: str
    start_date: str
    end_date: str