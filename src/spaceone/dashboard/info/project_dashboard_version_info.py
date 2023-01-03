import functools
from spaceone.api.dashboard.v1 import project_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model import ProjectDashboardVersion

__all__ = ['ProjectDashboardVersionInfo', 'ProjectDashboardVersionsInfo']


def ProjectDashboardVersionInfo(project_dashboard_version_vo: ProjectDashboardVersion, minimal=False,
                                latest_version=None):
    info = {
        'project_dashboard_id': project_dashboard_version_vo.project_dashboard_id,
        'version': project_dashboard_version_vo.version,
        'created_at': utils.datetime_to_iso8601(project_dashboard_version_vo.created_at),
        'domain_id': project_dashboard_version_vo.domain_id
    }

    if latest_version:
        if latest_version == project_dashboard_version_vo.version:
            info.update({
                'latest': True
            })
        else:
            info.update({
                'latest': False
            })

    if not minimal:
        info.update({
            'layouts': change_list_value_type(
                project_dashboard_version_vo.layouts) if project_dashboard_version_vo.layouts else None,
            'variables': change_struct_type(project_dashboard_version_vo.variables),
            'settings': change_struct_type(project_dashboard_version_vo.settings),
            'variables_schema': change_struct_type(project_dashboard_version_vo.variables_schema)
        })

    return project_dashboard_pb2.ProjectDashboardVersionInfo(**info)


def ProjectDashboardVersionsInfo(project_dashboard_version_vos, total_count, **kwargs):
    return project_dashboard_pb2.ProjectDashboardVersionsInfo(results=list(
        map(functools.partial(ProjectDashboardVersionInfo, **kwargs), project_dashboard_version_vos)),
        total_count=total_count)
