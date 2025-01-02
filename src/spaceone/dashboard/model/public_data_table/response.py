from datetime import datetime
from typing import Union, List, Literal
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["PublicDataTableResponse", "PublicDataTablesResponse"]

ResourceGroup = Literal["DOMAIN", "WORKSPACE", "PROJECT"]


class PublicDataTableResponse(BaseModel):
    data_table_id: Union[str, None] = None
    name: Union[str, None] = None
    state: Union[str, None] = None
    data_type: Union[str, None] = None
    source_type: Union[str, None] = None
    operator: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    labels_info: Union[dict, None] = None
    data_info: Union[dict, None] = None
    error_message: Union[str, None] = None
    dashboard_id: Union[str, None] = None
    widget_id: Union[str, None] = None
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


class PublicDataTablesResponse(BaseModel):
    results: List[PublicDataTableResponse]
    total_count: int
