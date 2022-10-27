import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import DomainDashboardManager
from spaceone.dashboard.model import DomainDashboard

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DomainDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_mgr: DomainDashboardManager = self.locator.get_manager('DomainDashboardManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """Register domain_dashboard

        Args:
            params (dict): {
                'name': 'str',
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
            for widget in params.get('layouts', []):
                widget['variable'] = default_variables

        return self.domain_dashboard_mgr.create_domain_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
    @check_required(['domain_dashboard_id', 'domain_id'])
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
                'domain_id': 'str'
            }

        Returns:
            domain_dashboard_vo (object)
        """

        domain_dashboard_id = params['domain_dashboard_id']
        domain_id = params['domain_id']
        layouts = params.get('layouts', [])
        options = params.get('options')
        default_variables = params.get('default_variables')

        if default_variables:
            for widget in layouts:
                widget['variable'] = default_variables

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id,
                                                                                              domain_id)
        return self.domain_dashboard_mgr.update_domain_dashboard_by_vo(params, domain_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
    @check_required(['domain_dashboard_id', 'domain_id'])
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
        self.domain_dashboard_mgr.delete_domain_dashboard(params['domain_dashboard_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
    @check_required(['domain_dashboard_id', 'domain_id'])
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
        domain_dashboard_id = params['domain_dashboard_id']
        domain_id = params['domain_id']

        return self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
    @check_required(['domain_id'])
    @append_query_filter(['domain_dashboard_id', 'name', 'labels', 'user_id', 'domain_id'])
    @append_keyword_filter(['domain_dashboard_id', 'name'])
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

        query = params.get('query', {})
        return self.domain_dashboard_mgr.list_domain_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN_OR_USER'})
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
