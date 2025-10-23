from pydantic import BaseModel


class TotalRequestModel(BaseModel):
    customers: list


class MediaRequestModel(BaseModel):
    customer: str

class GFATokenRequestModel(BaseModel):
    code : str
    state : str