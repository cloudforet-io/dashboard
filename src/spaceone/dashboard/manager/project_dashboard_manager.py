import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.project_dashboard_model import ProjectDashboard

_LOGGER = logging.getLogger(__name__)


class ProjectDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_model: ProjectDashboard = self.locator.get_model('ProjectDashboard')

    def create_project_dashboard(self, params):
        def _rollback(project_dashboard_vo):
            _LOGGER.info(f'[create_project_dashboard._rollback] '
                         f'Delete project_dashboard_vo : {project_dashboard_vo.name} '
                         f'({project_dashboard_vo.project_dashboard_id})')
            project_dashboard_vo.delete()

        project_dashboard_vo: ProjectDashboard = self.project_dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, project_dashboard_vo)

        return project_dashboard_vo

    def update_project_dashboard(self, params):
        project_dashboard_vo: ProjectDashboard = self.get_project_dashboard(params['project_dashboard_id'],
                                                                            params['domain_id'])
        return self.update_project_dashboard_by_vo(params, project_dashboard_vo)

    def update_project_dashboard_by_vo(self, params, project_dashboard_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_project_dashboard_by_vo._rollback] Revert Data : '
                         f'{old_data["project_dashboard_id"]}')
            project_dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, project_dashboard_vo.to_dict())
        return project_dashboard_vo.update(params)

    def delete_project_dashboard(self, project_dashboard_id, domain_id):
        project_dashboard_vo: ProjectDashboard = self.get_project_dashboard(project_dashboard_id, domain_id)
        project_dashboard_vo.delete()

    def get_project_dashboard(self, project_dashboard_id, domain_id, only=None):
        return self.project_dashboard_model.get(project_dashboard_id=project_dashboard_id, domain_id=domain_id,
                                                only=only)

    def list_project_dashboards(self, query=None):
        if query is None:
            query = {}
        return self.project_dashboard_model.query(**query)

    def stat_project_dashboards(self, query):
        return self.project_dashboard_model.stat(**query)

    def increase_version(self, project_dashboard_vo):
        def _rollback(vo: ProjectDashboard):
            _LOGGER.info(f'[increase_version._rollback] Decrease Version : '
                         f'{vo.project_dashboard_id}')
            vo.decrement('version')

        project_dashboard_vo.increment('version')
        self.transaction.add_rollback(_rollback, project_dashboard_vo)

    @staticmethod
    def delete_by_project_dashboard_vo(project_dashboard_vo):
        project_dashboard_vo.delete()
