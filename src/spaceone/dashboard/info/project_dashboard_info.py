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
        'labels': change_list_value_type(project_dashboard_vo.labels),
        'user_id': project_dashboard_vo.user_id,
        'domain_id': project_dashboard_vo.domain_id
    }

    if not minimal:
        info.update({
            'options': _ProjectDashboardOptionsInfo(project_dashboard_vo.options),
            'default_variables': change_struct_type(project_dashboard_vo.default_variables),
            'tags': change_struct_type(project_dashboard_vo.tags),
            'layouts': change_list_value_type(
                project_dashboard_vo.layouts) if project_dashboard_vo.layouts else None,
            'created_at': utils.datetime_to_iso8601(project_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(project_dashboard_vo.updated_at)
        })

        return project_dashboard_pb2.ProjectDashboardInfo(**info)


def ProjectDashboardsInfo(project_dashboard_vos, total_count, **kwargs):
    return project_dashboard_pb2.ProjectDashboardsInfo(results=list(
        map(functools.partial(ProjectDashboardInfo, **kwargs), project_dashboard_vos)), total_count=total_count)


def _PeriodInfo(period):
    if period:
        info = {
            'start': period.start,
            'end': period.end
        }
        return project_dashboard_pb2.ProjectDashboardPeriod(**info)
    else:
        return None


def _DateRangeInfo(date_range):
    if date_range:
        info = {
            'enabled': date_range.enabled,
            'period_type': date_range.period_type,
            'period': _PeriodInfo(date_range.period)
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


def _ProjectDashboardOptionsInfo(options):
    if options:
        info = {
            'date_range': _DateRangeInfo(options.date_range),
            'currency': _CurrencyInfo(options.currency)
        }

        return project_dashboard_pb2.ProjectDashboardOptions(**info)
    else:
        return None
