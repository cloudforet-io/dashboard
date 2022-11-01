import functools
from spaceone.api.dashboard.v1 import domain_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model.domain_dashboard_model import DomainDashboard

__all__ = ['DomainDashboardInfo', 'DomainDashboardsInfo']


def DomainDashboardInfo(domain_dashboard_vo: DomainDashboard, minimal=False):
    info = {
        'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
        'name': domain_dashboard_vo.name,
        'scope': domain_dashboard_vo.scope,
        'labels': change_list_value_type(domain_dashboard_vo.labels),
        'user_id': domain_dashboard_vo.user_id,
        'domain_id': domain_dashboard_vo.domain_id
    }

    if not minimal:
        info.update({
            'options': _DomainDashboardOptionsInfo(domain_dashboard_vo.options),
            'default_variables': change_struct_type(domain_dashboard_vo.default_variables),
            'tags': change_struct_type(domain_dashboard_vo.tags),
            'layouts': change_list_value_type(
                domain_dashboard_vo.layouts) if domain_dashboard_vo.layouts else None,
            'created_at': utils.datetime_to_iso8601(domain_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(domain_dashboard_vo.updated_at)
        })

        return domain_dashboard_pb2.DomainDashboardInfo(**info)


def DomainDashboardsInfo(domain_dashboard_vos, total_count, **kwargs):
    return domain_dashboard_pb2.DomainDashboardsInfo(results=list(
        map(functools.partial(DomainDashboardInfo, **kwargs), domain_dashboard_vos)), total_count=total_count)


def _PeriodInfo(period):
    if period:
        info = {
            'start': period.start,
            'end': period.end
        }
        return domain_dashboard_pb2.DomainDashboardPeriod(**info)
    else:
        return None


def _DateRangeInfo(date_range):
    if date_range:
        info = {
            'enabled': date_range.enabled,
            'period_type': date_range.period_type,
            'period': _PeriodInfo(date_range.period)
        }
        return domain_dashboard_pb2.DomainDashboardDateRange(**info)
    else:
        return None


def _CurrencyInfo(currency):
    if currency:
        info = {
            'enabled': currency.enabled
        }
        return domain_dashboard_pb2.DomainDashboardCurrency(**info)
    else:
        return None


def _DomainDashboardOptionsInfo(options):
    if options:
        info = {
            'date_range': _DateRangeInfo(options.date_range),
            'currency': _CurrencyInfo(options.currency)
        }

        return domain_dashboard_pb2.DomainDashboardOptions(**info)
    else:
        return None
