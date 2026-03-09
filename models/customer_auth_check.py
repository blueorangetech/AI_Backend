from pydantic import BaseModel


class Login_Info(BaseModel):
    user_id: str
    password: str
class Register_Info(BaseModel):
    user_id: str
    name: str
    password: str
