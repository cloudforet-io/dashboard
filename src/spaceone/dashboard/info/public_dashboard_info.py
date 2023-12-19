import functools
from spaceone.api.dashboard.v1 import public_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.public_dashboard_model import PublicDashboard

__all__ = ["PublicDashboardInfo", "PublicDashboardsInfo"]


def PublicDashboardInfo(dashboard_vo: PublicDashboard, minimal=False):
    info = {
        "public_dashboard_id": dashboard_vo.public_dashboard_id,
        "name": dashboard_vo.name,
        "version": dashboard_vo.version,
        "labels": change_list_value_type(dashboard_vo.labels),
        "resource_group": dashboard_vo.resource_group,
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

    return public_dashboard_pb2.PublicDashboardInfo(**info)


def PublicDashboardsInfo(dashboard_vos, total_count, **kwargs):
    return public_dashboard_pb2.PublicDashboardsInfo(
        results=list(
            map(functools.partial(PublicDashboardInfo, **kwargs), dashboard_vos)
        ),
        total_count=total_count,
    )
