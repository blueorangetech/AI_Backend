from pydantic import BaseModel


class Login_Info(BaseModel):
    name: str
    password: str
