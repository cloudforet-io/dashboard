from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "PublicDataTableAddRequest",
    "PublicDataTableTransformRequest",
    "PublicDataTableUpdateRequest",
    "PublicDataTableDeleteRequest",
    "PublicDataTableLoadRequest",
    "PublicDataTableGetRequest",
    "PublicDataTableSearchQueryRequest",
]

Granularity = Literal["DAILY", "MONTHLY", "YEARLY"]


class PublicDataTableAddRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    source_type: str
    options: dict
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableTransformRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    operator: str
    options: dict
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableUpdateRequest(BaseModel):
    data_table_id: str
    name: Union[str, None] = None
    options: Union[dict, None] = None
    vars: Union[dict, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableDeleteRequest(BaseModel):
    data_table_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableLoadRequest(BaseModel):
    data_table_id: str
    granularity: Granularity
    start: Union[str, None] = None
    end: Union[str, None] = None
    sort: Union[list, None] = None
    page: Union[dict, None] = None
    vars: Union[dict, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableGetRequest(BaseModel):
    data_table_id: str
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    widget_id: str
    data_table_id: Union[str, None] = None
    name: Union[str, None] = None
    data_type: Union[str, None] = None
    source_type: Union[str, None] = None
    operator: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDataTableStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
