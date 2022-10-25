import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import WidgetManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class WidgetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_mgr: WidgetManager = self.locator.get_manager('WidgetManager')

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
