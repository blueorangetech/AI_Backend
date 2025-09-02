from pydantic import BaseModel


class TotalRequestModel(BaseModel):
    customers: list


class MediaRequestModel(BaseModel):
    customer: str


class MetaAdsRequestModel(BaseModel):
    account_id: str
    table_name: str
    start_date: str | None = None
    end_date: str | None = None
