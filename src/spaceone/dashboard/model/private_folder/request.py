from typing import Union
from pydantic import BaseModel

__all__ = [
    "PrivateFolderCreateRequest",
    "PrivateFolderUpdateRequest",
    "PrivateFolderDeleteRequest",
    "PrivateFolderGetRequest",
    "PrivateFolderSearchQueryRequest",
    "PrivateFolderStatQueryRequest",
]


class PrivateFolderCreateRequest(BaseModel):
    name: str
    tags: Union[dict, None] = None
    dashboards: Union[list, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class PrivateFolderUpdateRequest(BaseModel):
    folder_id: str
    name: Union[str, None] = None
    tags: Union[dict, None] = None
    user_id: str
    domain_id: str


class PrivateFolderDeleteRequest(BaseModel):
    folder_id: str
    user_id: str
    domain_id: str


class PrivateFolderGetRequest(BaseModel):
    folder_id: str
    user_id: str
    domain_id: str


class PrivateFolderSearchQueryRequest(BaseModel):
    query: Union[dict, None] = None
    folder_id: Union[str, None] = None
    name: Union[str, None] = None
    user_id: str
    workspace_id: Union[str, None] = None
    domain_id: str


class PrivateFolderStatQueryRequest(BaseModel):
    query: dict
    user_id: str
    domain_id: str
