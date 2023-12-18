import functools
from spaceone.api.dashboard.v1 import dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.dashboard_model import Dashboard

__all__ = ["DashboardInfo", "DashboardsInfo"]


def DashboardInfo(dashboard_vo: Dashboard, minimal=False):
    info = {
        "dashboard_id": dashboard_vo.dashboard_id,
        "name": dashboard_vo.name,
        "dashboard_type": dashboard_vo.dashboard_type,
        "version": dashboard_vo.version,
        "labels": change_list_value_type(dashboard_vo.labels),
        "resource_group": dashboard_vo.resource_group,
        "user_id": dashboard_vo.user_id,
        "project_id": dashboard_vo.project_id,
        "workspace_id": dashboard_vo.workspace_id,
        "domain_id": dashboard_vo.domain_id,
    }

    if not minimal:
        info.update(
            {
                "layouts": change_list_value_type(dashboard_vo.layouts)
                if dashboard_vo.layouts
                else None,
                "variables": change_struct_type(dashboard_vo.variables),
                "settings": change_struct_type(dashboard_vo.settings),
                "variables_schema": change_struct_type(dashboard_vo.variables_schema),
                "tags": change_struct_type(dashboard_vo.tags),
                "created_at": utils.datetime_to_iso8601(dashboard_vo.created_at),
                "updated_at": utils.datetime_to_iso8601(dashboard_vo.updated_at),
            }
        )

    return dashboard_pb2.DashboardInfo(**info)


def DashboardsInfo(dashboard_vos, total_count, **kwargs):
    return dashboard_pb2.DashboardsInfo(
        results=list(map(functools.partial(DashboardInfo, **kwargs), dashboard_vos)),
        total_count=total_count,
    )
