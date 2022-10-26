import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.widget_model import Widget

_LOGGER = logging.getLogger(__name__)


class WidgetManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_model: Widget = self.locator.get_model('Widget')

    def create_widget(self, params):
        def _rollback(widget_vo):
            _LOGGER.info(f'[create_widget._rollback] '
                         f'Delete widget_vo : {widget_vo.name} '
                         f'({widget_vo.widget_id})')
            widget_vo.delete()

        widget_vo: Widget = self.widget_model.create(params)
        self.transaction.add_rollback(_rollback, widget_vo)

        return widget_vo

    def update_widget(self, params):
        widget_vo: Widget = self.get_widget(params['widget_id'], params['domain_id'])
        return self.update_widget_by_vo(params, widget_vo)

    def update_widget_by_vo(self, params, widget_vo):
        def _rollback(old_data):
            _LOGGER.info(f'[update_widget_by_vo._rollback] Revert Data : '
                         f'{old_data["widget_id"]}')
            widget_vo.update(old_data)

        self.transaction.add_rollback(_rollback, widget_vo.to_dict())
        return widget_vo.update(params)

    def delete_widget(self, widget_id, domain_id):
        widget_vo: Widget = self.get_widget(widget_id, domain_id)
        widget_vo.delete()

    def get_widget(self, widget_id, domain_id, only=None):
        return self.widget_model.get(widget_id=widget_id, domain_id=domain_id, only=only)

    def list_widgets(self, query=None):
        if query is None:
            query = {}
        return self.widget_model.query(**query)

    def stat_widgets(self, query):
        return self.widget_model.stat(**query)
