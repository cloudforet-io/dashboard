import functools
from spaceone.api.dashboard.v1 import project_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.project_dashboard_model import ProjectDashboard

__all__ = ['ProjectDashboardInfo', 'ProjectDashboardsInfo']


def ProjectDashboardInfo(project_dashboard_vo: ProjectDashboard, minimal=False):
    info = {
        'project_dashboard_id': project_dashboard_vo.project_dashboard_id,
        'name': project_dashboard_vo.name,
        'scope': project_dashboard_vo.scope,
        'options': change_struct_type(project_dashboard_vo.options),
        'default_variables': change_struct_type(project_dashboard_vo.default_variables),
        'labels': change_list_value_type(project_dashboard_vo.labels),
        'user_id': project_dashboard_vo.user_id,
        'domain_id': project_dashboard_vo.domain_id
    }

    if not minimal:
        info.update({
            'layouts': change_list_value_type(
                project_dashboard_vo.layouts) if project_dashboard_vo.layouts else None,
            'created_at': utils.datetime_to_iso8601(project_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(project_dashboard_vo.updated_at)
        })

        return project_dashboard_pb2.ProjectDashboardInfo(**info)


def ProjectDashboardsInfo(project_dashboard_vos, total_count, **kwargs):
    return project_dashboard_pb2.ProjectDashboardInfo(results=list(
        map(functools.partial(ProjectDashboardInfo, **kwargs), project_dashboard_vos)), total_count=total_count)
