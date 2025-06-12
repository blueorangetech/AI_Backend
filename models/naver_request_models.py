from pydantic import BaseModel

class NaverRequsetModel(BaseModel):
    customer_id : str
    target_date: str