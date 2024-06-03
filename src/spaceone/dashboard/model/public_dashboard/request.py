from typing import Union, List, Literal, Any
from pydantic import BaseModel

__all__ = [
    "PublicDashboardCreateRequest",
    "PublicDashboardUpdateRequest",
    "PublicDashboardDeleteRequest",
    "PublicDashboardGetRequest",
    "PublicDashboardSearchQueryRequest",
    "PublicDashboardStatQueryRequest",
    "ResourceGroup",
]

ResourceGroup = Literal["DOMAIN", "WORKSPACE", "PROJECT"]


class PublicDashboardCreateRequest(BaseModel):
    name: str
    description: Union[str, None] = None
    layouts: Union[List[Any], None] = None
    vars: Union[dict, None] = None
    options: Union[dict, None] = None
    variables: Union[dict, None] = None
    variables_schema: Union[dict, None] = None
    labels: Union[List[str], None] = None
    tags: Union[dict, None] = None
    folder_id: Union[str, None] = None
    resource_group: ResourceGroup
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDashboardUpdateRequest(BaseModel):
    dashboard_id: str
    name: Union[str, None] = None
    description: Union[str, None] = None
    layouts: Union[List[Any], None] = None
    vars: Union[dict, None] = None
    settings: Union[dict, None] = None
    variables: Union[dict, None] = None
    variables_schema: Union[dict, None] = None
    labels: Union[List[str], None] = None
    tags: Union[dict, None] = None
    folder_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDashboardDeleteRequest(BaseModel):
    dashboard_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDashboardGetRequest(BaseModel):
    dashboard_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDashboardSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    dashboard_id: Union[str, None] = None
    name: Union[str, None] = None
    folder_id: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicDashboardStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
