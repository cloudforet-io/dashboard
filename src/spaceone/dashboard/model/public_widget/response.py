from datetime import datetime
from typing import Union, List, Literal
from pydantic import BaseModel
from spaceone.core import utils

__all__ = ["PublicWidgetResponse", "PublicWidgetsResponse"]

ResourceGroup = Literal["DOMAIN", "WORKSPACE", "PROJECT"]


class PublicWidgetResponse(BaseModel):
    widget_id: Union[str, None] = None
    name: Union[str, None] = None
    state: Union[str, None] = None
    description: Union[str, None] = None
    widget_type: Union[str, None] = None
    size: Union[str, None] = None
    options: Union[dict, None] = None
    tags: Union[dict, None] = None
    data_table_id: Union[str, None] = None
    dashboard_id: Union[str, None] = None
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


class PublicWidgetsResponse(BaseModel):
    results: List[PublicWidgetResponse]
    total_count: int
