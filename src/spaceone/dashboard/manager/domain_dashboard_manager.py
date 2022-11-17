import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.domain_dashboard_model import DomainDashboard

_LOGGER = logging.getLogger(__name__)


class DomainDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_model: DomainDashboard = self.locator.get_model('DomainDashboard')

    def create_domain_dashboard(self, params):
        def _rollback(domain_dashboard_vo):
            _LOGGER.info(f'[create_domain_dashboard._rollback] '
                         f'Delete domain_dashboard_vo : {domain_dashboard_vo.name} '
                         f'({domain_dashboard_vo.domain_dashboard_id})')
            domain_dashboard_vo.delete()

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, domain_dashboard_vo)

        return domain_dashboard_vo

    def update_domain_dashboard(self, params):
        domain_dashboard_vo: DomainDashboard = self.get_domain_dashboard(params['domain_dashboard_id'],
                                                                         params['domain_id'])
        return self.update_domain_dashboard_by_vo(params, domain_dashboard_vo)

    def update_domain_dashboard_by_vo(self, params, domain_dashboard_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_domain_dashboard_by_vo._rollback] Revert Data : '
                         f'{old_data["domain_dashboard_id"]}')
            domain_dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, domain_dashboard_vo.to_dict())
        return domain_dashboard_vo.update(params)

    def delete_domain_dashboard(self, domain_dashboard_id, domain_id):
        domain_dashboard_vo: DomainDashboard = self.get_domain_dashboard(domain_dashboard_id, domain_id)
        domain_dashboard_vo.delete()

    def get_domain_dashboard(self, domain_dashboard_id, domain_id, only=None):
        return self.domain_dashboard_model.get(domain_dashboard_id=domain_dashboard_id, domain_id=domain_id, only=only)

    def list_domain_dashboards(self, query=None):
        if query is None:
            query = {}
        return self.domain_dashboard_model.query(**query)

    def stat_domain_dashboards(self, query):
        return self.domain_dashboard_model.stat(**query)

    def increase_version(self, domain_dashboard_vo):
        def _rollback(vo: DomainDashboard):
            _LOGGER.info(f'[increase_version._rollback] Decrease Version : '
                         f'{vo.domain_dashboard_id}')
            vo.decrement('version')

        domain_dashboard_vo.increment('version')
        self.transaction.add_rollback(_rollback, domain_dashboard_vo)

    @staticmethod
    def delete_by_domain_dashboard_vo(domain_dashboard_vo):
        domain_dashboard_vo.delete()
