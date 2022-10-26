import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import DomainDashboardManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DomainDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_mgr: DomainDashboardManager = self.locator.get_manager('DomainDashboardManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'scope', 'domain_id'])
    @change_date_value(['start', 'end'])
    def create(self, params):
        """Register domain_dashboard

        Args:
            params (dict): {
                'name': 'str',
                'scope': 'str',
                'layouts': 'list',
                'options': 'dict',
                'default_variables': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'user_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            domain_dashboard_vo (object)
        """

        if params.get('user_id'):
            params['scope'] = 'USER'
        else:
            params['scope'] = 'DOMAIN'

        if default_variables := params.get('default_variables'):
            for widget in params.get('layouts'):
                widget['variable'] = default_variables

        return self.domain_dashboard_mgr.create_domain_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['public_dashboard_id', 'domain_id'])
    @change_date_value(['end'])
    def update(self, params):
        """Update domain_dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'name': 'str',
                'layouts': 'list',
                'options': 'dict',
                'default_variables': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'user_id': 'str', # TODO : have to delete user_id
                'domain_id': 'str'
            }

        Returns:
            domain_dashboard_vo (object)
        """
        pass

    def delete(self, params):
        """Deregister domain_dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """
        pass

    def get(self, params):
        """ Get domain_dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            domain_dashboard_vo (object)
        """
        pass

    def list(self, params):
        """ List public_dashboards

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'name': 'str',
                'scope': 'str',
                'labels': 'list',
                'user_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            domain_dashboard_vos (object)
            total_count
        """
        pass

    @transaction(append_meta={'authorization.scope': 'USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['domain_dashboard_id', 'name'])
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
        return self.domain_dashboard_mgr.stat_domain_dashboards(query)
