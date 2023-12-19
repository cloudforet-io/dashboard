import functools
from spaceone.api.dashboard.v1 import private_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model import PrivateDashboardVersion

__all__ = ["PrivateDashboardVersionInfo", "PrivateDashboardVersionsInfo"]


def PrivateDashboardVersionInfo(
    dashboard_version_vo: PrivateDashboardVersion, minimal=False, latest_version=None
):
    info = {
        "private_dashboard_id": dashboard_version_vo.private_dashboard_id,
        "version": dashboard_version_vo.version,
        "created_at": utils.datetime_to_iso8601(dashboard_version_vo.created_at),
        "domain_id": dashboard_version_vo.domain_id,
    }

    if latest_version:
        if latest_version == dashboard_version_vo.version:
            info.update({"latest": True})
        else:
            info.update({"latest": False})

    if not minimal:
        info.update(
            {
                "layouts": change_list_value_type(dashboard_version_vo.layouts)
                if dashboard_version_vo.layouts
                else None,
                "variables": change_struct_type(dashboard_version_vo.variables),
                "settings": change_struct_type(dashboard_version_vo.settings),
                "variables_schema": change_struct_type(
                    dashboard_version_vo.variables_schema
                ),
            }
        )

    return private_dashboard_pb2.PrivateDashboardVersionInfo(**info)


def PrivateDashboardVersionsInfo(dashboard_version_vos, total_count, **kwargs):
    return private_dashboard_pb2.PrivateDashboardVersionsInfo(
        results=list(
            map(
                functools.partial(PrivateDashboardVersionInfo, **kwargs),
                dashboard_version_vos,
            )
        ),
        total_count=total_count,
    )
