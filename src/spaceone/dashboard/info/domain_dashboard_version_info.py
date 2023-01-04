import functools
from spaceone.api.dashboard.v1 import domain_dashboard_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils
from spaceone.dashboard.model import DomainDashboardVersion

__all__ = ['DomainDashboardVersionInfo', 'DomainDashboardVersionsInfo']


def DomainDashboardVersionInfo(domain_dashboard_version_vo: DomainDashboardVersion, minimal=False, latest_version=None):
    info = {
        'domain_dashboard_id': domain_dashboard_version_vo.domain_dashboard_id,
        'version': domain_dashboard_version_vo.version,
        'created_at': utils.datetime_to_iso8601(domain_dashboard_version_vo.created_at),
        'domain_id': domain_dashboard_version_vo.domain_id
    }

    if latest_version:
        if latest_version == domain_dashboard_version_vo.version:
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
                domain_dashboard_version_vo.layouts) if domain_dashboard_version_vo.layouts else None,
            'variables': change_struct_type(domain_dashboard_version_vo.variables),
            'settings': change_struct_type(domain_dashboard_version_vo.settings),
            'variables_schema': change_struct_type(domain_dashboard_version_vo.variables_schema)
        })

    return domain_dashboard_pb2.DomainDashboardVersionInfo(**info)


def DomainDashboardVersionsInfo(domain_dashboard_version_vos, total_count, **kwargs):
    return domain_dashboard_pb2.DomainDashboardVersionsInfo(results=list(
        map(functools.partial(DomainDashboardVersionInfo, **kwargs), domain_dashboard_version_vos)),
        total_count=total_count)
