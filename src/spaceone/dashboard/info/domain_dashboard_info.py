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
        'options': change_struct_type(domain_dashboard_vo.options),
        'default_variables': change_struct_type(domain_dashboard_vo.default_variables),
        'labels': change_list_value_type(domain_dashboard_vo.labels),
        'user_id': domain_dashboard_vo.user_id,
        'domain_id': domain_dashboard_vo.domain_id
    }

    if not minimal:
        info.update({
            'layouts': change_list_value_type(
                domain_dashboard_vo.layouts) if domain_dashboard_vo.layouts else None,
            'created_at': utils.datetime_to_iso8601(domain_dashboard_vo.created_at),
            'updated_at': utils.datetime_to_iso8601(domain_dashboard_vo.updated_at)
        })

        return domain_dashboard_pb2.DomainDashboardInfo(**info)


def DomainDashboardsInfo(domain_dashboard_vos, total_count, **kwargs):
    return domain_dashboard_pb2.DomainDashboardsInfo(results=list(
        map(functools.partial(DomainDashboardInfo, **kwargs), domain_dashboard_vos)), total_count=total_count)
