import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model import DomainDashboardVersion

_LOGGER = logging.getLogger(__name__)


class DomainDashboardVersionManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_version_model: DomainDashboardVersion = self.locator.get_model('DomainDashboardVersion')

    def create_version(self, params):
        def _rollback(version_vo):
            _LOGGER.info(f'[create_domain_dashboard_version._rollback] '
                         f'Delete domain_dashboard_version_vo : {version_vo.version} '
                         f'({version_vo.domain_dashboard_id})')
            version_vo.delete()

        version_vo: DomainDashboardVersion = self.domain_dashboard_version_model.create(params)
        self.transaction.add_rollback(_rollback, version_vo)

        return version_vo

    def delete_version(self, domain_dashboard_id, version, domain_id):
        version_vo: DomainDashboardVersion = self.get_version(domain_dashboard_id, version, domain_id)
        version_vo.delete()

    def get_version(self, domain_dashboard_id, version, domain_id, only=None):
        return self.domain_dashboard_version_model.get(domain_dashboard_id=domain_dashboard_id, version=version,
                                                       domain_id=domain_id, only=only)

    def list_versions(self, query=None):
        if query is None:
            query = {}
        return self.domain_dashboard_version_model.query(**query)