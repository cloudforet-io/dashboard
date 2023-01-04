import copy
import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import CustomWidgetManager
from spaceone.dashboard.model import CustomWidget

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class CustomWidgetService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_widget_mgr: CustomWidgetManager = self.locator.get_manager('CustomWidgetManager')

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['widget_name', 'title', 'options', 'domain_id'])
    def create(self, params):
        """Register widget

        Args:
            params (dict): {
                'widget_name': 'str',
                'title': 'str',
                'options': 'dict',
                'settings': 'dict',
                'inherit_options': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """

        params['user_id'] = self.transaction.get_meta('user_id')

        return self.custom_widget_mgr.create_custom_widget(params)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    def update(self, params):
        """Update widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'title': 'str',
                'options': 'dict',
                'settings': 'dict',
                'inherit_option': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """
        custom_widget_id = params['custom_widget_id']
        domain_id = params['domain_id']

        custom_widget_vo: CustomWidget = self.custom_widget_mgr.get_custom_widget(custom_widget_id, domain_id)

        if 'settings' in params:
            params['settings'] = self._merge_settings(custom_widget_vo.settings, params['settings'])

        return self.custom_widget_mgr.update_custom_widget_by_vo(params, custom_widget_vo)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    def delete(self, params):
        """Deregister widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """
        return self.custom_widget_mgr.delete_custom_widget(params['custom_widget_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['custom_widget_id', 'domain_id'])
    def get(self, params):
        """Get widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'only': 'list',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vo (object)
        """

        return self.custom_widget_mgr.get_custom_widget(params['custom_widget_id'], params['domain_id'],
                                                        params.get('only'))

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['domain_id'])
    @append_query_filter(['custom_widget_id', 'widget_name', 'title', 'user_id', 'domain_id'])
    @append_keyword_filter(['custom_widget_id', 'title'])
    def list(self, params):
        """List widget

        Args:
            params (dict): {
                'custom_widget_id': 'str',
                'widget_name': 'str',
                'title': 'str',
                'user_id': 'str',
                'query': 'dict',
                'domain_id': 'str'
            }

        Returns:
            custom_widget_vos (object)
        """
        query = params.get('query', {})
        return self.custom_widget_mgr.list_custom_widgets(query)

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['custom_widget_id', 'title'])
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
        return self.custom_widget_mgr.stat_custom_widgets(query)

    @staticmethod
    def _merge_settings(old_settings, new_settings):
        settings = copy.deepcopy(old_settings)

        if old_settings:
            settings.update(new_settings)
            return settings
        else:
            return new_settings
