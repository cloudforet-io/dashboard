import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import WidgetManager
from spaceone.dashboard.model import Widget

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class WidgetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_mgr: WidgetManager = self.locator.get_manager('WidgetManager')

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """Register widget

        Args:
            params (dict): {
                'name': 'str',
                'options': 'str',
                'variables': 'str',
                'schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            widget_vo (object)
        """

        params['user_id'] = self.transaction.get_meta('user_id')

        return self.widget_mgr.create_widget(params)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['widget_id', 'domain_id'])
    def update(self, params):
        """Update widget

        Args:
            params (dict): {
                'widget_id': 'str',
                'name': 'str',
                'options': 'str',
                'variables': 'dict',
                'schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """
        widget_id = params['widget_id']
        domain_id = params['domain_id']

        widget_vo: Widget = self.widget_mgr.get_widget(widget_id, domain_id)

        return self.widget_mgr.update_widget_by_vo(params, widget_vo)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['widget_id', 'domain_id'])
    def delete(self, params):
        """Deregister widget

        Args:
            params (dict): {
                'widget_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            widget_vo (object)
        """
        return self.widget_mgr.delete_widget(params['widget_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['widget_id', 'domain_id'])
    def get(self, params):
        """Get widget

        Args:
            params (dict): {
                'widget_id': 'str',
                'only': 'list',
                'domain_id': 'str'
            }

        Returns:
            widget_vo (object)
        """

        return self.widget_mgr.get_widget(params['widget_id'], params['domain_id'], params.get('only'))

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['domain_id'])
    @append_query_filter(['widget_id', 'name', 'user_id', 'domain_id'])
    @append_keyword_filter(['widget_id', 'name'])
    def list(self, params):
        """List widget

        Args:
            params (dict): {
                'widget_id': 'str',
                'name': 'str',
                'user_id': 'str',
                'query': 'dict',
                'domain_id': 'str'
            }

        Returns:
            widget_vos (object)
        """
        query = params.get('query', {})
        return self.widget_mgr.list_widgets(query)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['widget_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        query = params.get('query', {})
        return self.widget_mgr.stat_widgets(query)

