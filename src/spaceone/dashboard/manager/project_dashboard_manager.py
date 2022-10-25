import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.project_dashboard_model import ProjectDashboard

_LOGGER = logging.getLogger(__name__)


class ProjectDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_model: ProjectDashboard = self.locator.get_model('ProjectDashboard')
