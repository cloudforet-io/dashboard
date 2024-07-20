from typing import Union
from pydantic import BaseModel

__all__ = [
    "PrivateWidgetCreateRequest",
    "PrivateWidgetUpdateRequest",
    "PrivateWidgetDeleteRequest",
    "PrivateWidgetLoadRequest",
    "PrivateWidgetGetRequest",
    "PrivateWidgetSearchQueryRequest",
]


class PrivateWidgetCreateRequest(BaseModel):
    dashboard_id: str
    name: Union[str, None] = None
    state: Union[str, None] = None
    description: Union[str, None] = None
    widget_type: Union[str, None] = None
    size: Union[str, None] = None
    options: Union[dict, None] = None
    data_table_id: Union[int, None] = None
    data_tables: Union[list, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateWidgetUpdateRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    state: Union[str, None] = None
    description: Union[str, None] = None
    widget_type: Union[str, None] = None
    size: Union[str, None] = None
    options: Union[dict, None] = None
    data_table_id: Union[str, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateWidgetDeleteRequest(BaseModel):
    widget_id: str
    user_id: str
    domain_id: str


class PrivateWidgetLoadRequest(BaseModel):
    widget_id: str
    query: dict
    vars: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateWidgetGetRequest(BaseModel):
    widget_id: str
    user_id: str
    domain_id: str


class PrivateWidgetSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    dashboard_id: str
    widget_id: Union[str, None] = None
    name: Union[str, None] = None
    user_id: str
    domain_id: str


class PrivateWidgetStatQueryRequest(BaseModel):
    query: dict
    user_id: str
    domain_id: str
