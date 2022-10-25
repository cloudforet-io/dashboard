import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.domain_dashboard_model import DomainDashboard

_LOGGER = logging.getLogger(__name__)


class DomainDashboardManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_model: DomainDashboard = self.locator.get_model('DomainDashboard')
