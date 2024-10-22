from datetime import datetime
from typing import Union, List, Any
from pydantic import BaseModel
from spaceone.core import utils

from spaceone.dashboard.model.public_dashboard.request import ResourceGroup, Scope

__all__ = ["PublicDashboardResponse", "PublicDashboardsResponse"]


class PublicDashboardResponse(BaseModel):
    dashboard_id: Union[str, None] = None
    name: Union[str, None] = None
    description: Union[str, None] = None
    version: Union[str, None] = None
    layouts: Union[List[Any], None] = None
    vars: Union[dict, None] = None
    vars_schema: Union[dict, None] = None
    options: Union[dict, None] = None
    variables: Union[dict, None] = None
    variables_schema: Union[dict, None] = None
    labels: Union[List[str], None] = None
    tags: Union[dict, None] = None
    shared: Union[bool, None] = None
    scope: Union[str, None] = None
    folder_id: Union[str, None] = None
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


class PublicDashboardsResponse(BaseModel):
    results: List[PublicDashboardResponse]
    total_count: int
