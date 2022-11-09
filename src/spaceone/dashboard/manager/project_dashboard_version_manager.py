import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model import ProjectDashboardVersion

_LOGGER = logging.getLogger(__name__)


class ProjectDashboardVersionManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_version_model: ProjectDashboardVersion = self.locator.get_model(
            'ProjectDashboardVersion')

    def create_version(self, params):
        def _rollback(version_vo):
            _LOGGER.info(f'[create_project_dashboard_version._rollback] '
                         f'Delete project_dashboard_version_vo : {version_vo.version} '
                         f'({version_vo.project_dashboard_id})')
            version_vo.delete()

        version_vo: ProjectDashboardVersion = self.project_dashboard_version_model.create(params)
        self.transaction.add_rollback(_rollback, version_vo)

        return version_vo

    def delete_version(self, project_dashboard_id, version, domain_id):
        version_vo: ProjectDashboardVersion = self.get_version(project_dashboard_id, version, domain_id)
        version_vo.delete()

    def get_version(self, project_dashboard_id, version, domain_id, only=None):
        return self.project_dashboard_version_model.get(project_dashboard_id=project_dashboard_id, version=version,
                                                        domain_id=domain_id, only=only)

    def list_versions(self, query=None):
        if query is None:
            query = {}
        return self.project_dashboard_version_model.query(**query)
