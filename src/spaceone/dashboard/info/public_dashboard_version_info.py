import functools
from spaceone.api.dashboard.v1 import public_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model import PublicDashboardVersion

__all__ = ["PublicDashboardVersionInfo", "PublicDashboardVersionsInfo"]


def PublicDashboardVersionInfo(
    dashboard_version_vo: PublicDashboardVersion, minimal=False, latest_version=None
):
    info = {
        "public_dashboard_id": dashboard_version_vo.public_dashboard_id,
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

    return public_dashboard_pb2.PublicDashboardVersionInfo(**info)


def PublicDashboardVersionsInfo(dashboard_version_vos, total_count, **kwargs):
    return public_dashboard_pb2.PublicDashboardVersionsInfo(
        results=list(
            map(
                functools.partial(PublicDashboardVersionInfo, **kwargs),
                dashboard_version_vos,
            )
        ),
        total_count=total_count,
    )
