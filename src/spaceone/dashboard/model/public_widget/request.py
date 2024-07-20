from typing import Union
from pydantic import BaseModel

__all__ = [
    "PublicWidgetCreateRequest",
    "PublicWidgetUpdateRequest",
    "PublicWidgetDeleteRequest",
    "PublicWidgetLoadRequest",
    "PublicWidgetGetRequest",
    "PublicWidgetSearchQueryRequest",
]


class PublicWidgetCreateRequest(BaseModel):
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
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetUpdateRequest(BaseModel):
    widget_id: str
    name: Union[str, None] = None
    state: Union[str, None] = None
    description: Union[str, None] = None
    widget_type: Union[str, None] = None
    size: Union[str, None] = None
    options: Union[dict, None] = None
    data_table_id: Union[str, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetDeleteRequest(BaseModel):
    widget_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetGetRequest(BaseModel):
    widget_id: str
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetLoadRequest(BaseModel):
    widget_id: str
    query: dict
    vars: Union[dict, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    dashboard_id: str
    widget_id: Union[str, None] = None
    name: Union[str, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicWidgetStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
