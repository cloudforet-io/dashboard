import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.widget_model import Widget

_LOGGER = logging.getLogger(__name__)


class WidgetManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_model: Widget = self.locator.get_model('Widget')
