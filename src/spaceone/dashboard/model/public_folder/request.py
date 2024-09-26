from typing import Union, Literal
from pydantic import BaseModel

__all__ = [
    "PublicFolderCreateRequest",
    "PublicFolderUpdateRequest",
    "PublicFolderShareRequest",
    "PublicFolderUnshareRequest",
    "PublicFolderDeleteRequest",
    "PublicFolderGetRequest",
    "PublicFolderSearchQueryRequest",
    "PublicFolderStatQueryRequest",
    "ResourceGroup",
]

ResourceGroup = Literal["DOMAIN", "WORKSPACE", "PROJECT"]
Scope = Literal["WORKSPACE", "PROJECT"]


class PublicFolderCreateRequest(BaseModel):
    name: str
    tags: Union[dict, None] = None
    dashboards: Union[list, None] = None
    resource_group: ResourceGroup
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str


class PublicFolderUpdateRequest(BaseModel):
    folder_id: str
    name: Union[str, None] = None
    tags: Union[dict, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderShareRequest(BaseModel):
    folder_id: str
    scope: Union[Scope, None] = None
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderUnshareRequest(BaseModel):
    folder_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderDeleteRequest(BaseModel):
    folder_id: str
    workspace_id: Union[str, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderGetRequest(BaseModel):
    folder_id: str
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    folder_id: Union[str, None] = None
    name: Union[str, None] = None
    shared: Union[bool, None] = None
    scope: Union[Scope, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None


class PublicFolderStatQueryRequest(BaseModel):
    query: dict
    workspace_id: Union[str, list, None] = None
    domain_id: str
    user_projects: Union[list, None] = None
