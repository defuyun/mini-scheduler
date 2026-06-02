from pydantic import BaseModel

class CoordinatorInfo(BaseModel):
    service_name: str