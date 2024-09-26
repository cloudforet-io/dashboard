from datetime import datetime
from typing import Union, List
from pydantic import BaseModel
from spaceone.core import utils

from spaceone.dashboard.model.public_folder.request import ResourceGroup

__all__ = ["PublicFolderResponse", "PublicFoldersResponse"]


class PublicFolderResponse(BaseModel):
    folder_id: Union[str, None] = None
    name: Union[str, None] = None
    tags: Union[dict, None] = None
    shared: Union[bool, None] = None
    scope: Union[str, None] = None
    resource_group: Union[ResourceGroup, None] = None
    project_id: Union[str, None] = None
    workspace_id: Union[str, None] = None
    domain_id: Union[str, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None

    def dict(self, *args, **kwargs):
        data = super().dict(*args, **kwargs)
        data["created_at"] = utils.datetime_to_iso8601(data["created_at"])
        data["updated_at"] = utils.datetime_to_iso8601(data["updated_at"])
        return data


class PublicFoldersResponse(BaseModel):
    results: List[PublicFolderResponse]
    total_count: int
