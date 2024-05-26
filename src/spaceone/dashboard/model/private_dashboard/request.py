from typing import Union, List, Any
from pydantic import BaseModel

__all__ = [
    "PrivateDashboardCreateRequest",
    "PrivateDashboardUpdateRequest",
    "PrivateDashboardDeleteRequest",
    "PrivateDashboardGetRequest",
    "PrivateDashboardSearchQueryRequest",
    "PrivateDashboardStatQueryRequest",
]


class PrivateDashboardCreateRequest(BaseModel):
    name: str
    layouts: Union[List[Any], None] = None
    vars: Union[dict, None] = None
    settings: Union[dict, None] = None
    variables: Union[dict, None] = None
    variables_schema: Union[dict, None] = None
    labels: Union[List[str], None] = None
    tags: Union[dict, None] = None
    user_id: str
    workspace_id: str
    domain_id: str


class PrivateDashboardUpdateRequest(BaseModel):
    private_dashboard_id: str
    name: Union[str, None] = None
    layouts: Union[List[Any], None] = None
    vars: Union[dict, None] = None
    settings: Union[dict, None] = None
    variables: Union[dict, None] = None
    variables_schema: Union[dict, None] = None
    labels: Union[List[str], None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateDashboardDeleteRequest(BaseModel):
    private_dashboard_id: str
    user_id: str
    domain_id: str


class PrivateDashboardGetRequest(BaseModel):
    private_dashboard_id: str
    user_id: str
    domain_id: str


class PrivateDashboardSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    private_dashboard_id: Union[str, None] = None
    name: Union[str, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class PrivateDashboardStatQueryRequest(BaseModel):
    query: dict
    user_id: str
    domain_id: str
