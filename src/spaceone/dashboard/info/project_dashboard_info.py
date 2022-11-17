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
        'viewers': project_dashboard_vo.viewers,
        'version': project_dashboard_vo.version,
        'labels': change_list_value_type(project_dashboard_vo.labels),
        'project_id': project_dashboard_vo.project_id,
        'user_id': project_dashboard_vo.user_id,
        'domain_id': project_dashboard_vo.domain_id
    }

    if not minimal:
        info.update({
            'layouts': change_list_value_type(
                project_dashboard_vo.layouts) if project_dashboard_vo.layouts else None,
            'dashboard_options': change_struct_type(project_dashboard_vo.dashboard_options),
            'settings': _ProjectDashboardSettingsInfo(project_dashboard_vo.settings),
            'dashboard_options_schema': change_struct_type(project_dashboard_vo.dashboard_options_schema),
            'tags': change_struct_type(project_dashboard_vo.tags),
            'created_at': utils.datetime_to_iso8601(project_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(project_dashboard_vo.updated_at)
        })

    return project_dashboard_pb2.ProjectDashboardInfo(**info)


def ProjectDashboardsInfo(project_dashboard_vos, total_count, **kwargs):
    return project_dashboard_pb2.ProjectDashboardsInfo(results=list(
        map(functools.partial(ProjectDashboardInfo, **kwargs), project_dashboard_vos)), total_count=total_count)


def _DateRangeInfo(date_range):
    if date_range:
        info = {
            'enabled': date_range.enabled,
        }
        return project_dashboard_pb2.ProjectDashboardDateRange(**info)
    else:
        return None


def _CurrencyInfo(currency):
    if currency:
        info = {
            'enabled': currency.enabled
        }
        return project_dashboard_pb2.ProjectDashboardCurrency(**info)
    else:
        return None


def _ProjectDashboardSettingsInfo(options):
    if options:
        info = {
            'date_range': _DateRangeInfo(options.date_range),
            'currency': _CurrencyInfo(options.currency)
        }

        return project_dashboard_pb2.ProjectDashboardSettings(**info)
    else:
        return None
