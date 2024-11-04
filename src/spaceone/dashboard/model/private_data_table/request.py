from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "PrivateDataTableAddRequest",
    "PrivateDataTableTransformRequest",
    "PrivateDataTableUpdateRequest",
    "PrivateDataTableDeleteRequest",
    "PrivateDataTableLoadRequest",
    "PrivateDataTableGetRequest",
    "PrivateDataTableSearchQueryRequest",
]

Granularity = Literal["DAILY", "MONTHLY", "YEARLY"]


class PrivateDataTableAddRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    source_type: str
    options: dict
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateDataTableTransformRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    operator: str
    options: dict
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateDataTableUpdateRequest(BaseModel):
    data_table_id: str
    name: Union[str, None] = None
    options: Union[dict, None] = None
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateDataTableDeleteRequest(BaseModel):
    data_table_id: str
    user_id: str
    domain_id: str


class PrivateDataTableLoadRequest(BaseModel):
    data_table_id: str
    granularity: Granularity
    start: Union[str, None] = None
    end: Union[str, None] = None
    sort: Union[list, None] = None
    page: Union[dict, None] = None
    vars: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateDataTableGetRequest(BaseModel):
    data_table_id: str
    user_id: str
    domain_id: str


class PrivateDataTableSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    widget_id: str
    data_table_id: Union[str, None] = None
    name: Union[str, None] = None
    data_type: Union[str, None] = None
    source_type: Union[str, None] = None
    operator: Union[str, None] = None
    user_id: str
    domain_id: str


class PrivateDataTableStatQueryRequest(BaseModel):
    query: dict
    user_id: str
    domain_id: str
