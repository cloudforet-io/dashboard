import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model import DomainDashboardVersion

_LOGGER = logging.getLogger(__name__)


class DomainDashboardVersionManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_version_model: DomainDashboardVersion = self.locator.get_model('DomainDashboardVersion')

    def create_version_by_domain_dashboard_vo(self, domain_dashboard_vo, params):
        def _rollback(version_vo):
            _LOGGER.info(f'[create_domain_dashboard_version._rollback] '
                         f'Delete domain_dashboard_version_vo : {version_vo.version} '
                         f'({version_vo.domain_dashboard_id})')
            version_vo.delete()

        new_params = {
            'domain_dashboard_id': domain_dashboard_vo.domain_dashboard_id,
            'version': domain_dashboard_vo.version,
            'layouts': params.get('layouts') if params.get('layouts') else domain_dashboard_vo.layouts,
            'variables': params.get('variables') if params.get(
                'variables') else domain_dashboard_vo.variables,
            'settings': params.get('settings') if params.get('settings') else domain_dashboard_vo.settings.to_dict(),
            'variables_schema': params.get('variables_schema') if params.get(
                'variables_schema') else domain_dashboard_vo.variables_schema,
            'domain_id': domain_dashboard_vo.domain_id
        }

        version_vo: DomainDashboardVersion = self.domain_dashboard_version_model.create(new_params)
        self.transaction.add_rollback(_rollback, version_vo)
        return version_vo

    def delete_version(self, domain_dashboard_id, version, domain_id):
        version_vo: DomainDashboardVersion = self.get_version(domain_dashboard_id, version, domain_id)
        version_vo.delete()

    def delete_versions_by_domain_dashboard_version_vos(self, domain_dashboard_version_vos):
        domain_dashboard_version_vos.delete()

    def get_version(self, domain_dashboard_id, version, domain_id, only=None):
        return self.domain_dashboard_version_model.get(domain_dashboard_id=domain_dashboard_id, version=version,
                                                       domain_id=domain_id, only=only)

    def list_versions(self, query=None):
        if query is None:
            query = {}
        return self.domain_dashboard_version_model.query(**query)

    def filter_versions(self, **conditions):
        return self.domain_dashboard_version_model.filter(**conditions)
