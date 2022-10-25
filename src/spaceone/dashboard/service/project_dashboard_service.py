import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import ProjectDashboardManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ProjectDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_mgr: ProjectDashboardManager = self.locator.get_manager('ProjectDashboardManager')

    def create(self, params):
        pass

    def update(self, params):
        pass

    def delete(self, params):
        pass

    def get(self, params):
        pass

    def list(self, params):
        pass

    def stat(self, params):
        pass
