import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import ProjectDashboardManager
from spaceone.dashboard.model import ProjectDashboard

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ProjectDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_mgr: ProjectDashboardManager = self.locator.get_manager('ProjectDashboardManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['name', 'domain_id'])
    def create(self, params):
        """Register project_dashboard

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
            project_dashboard_vo (object)
        """

        if params.get('user_id'):
            params['scope'] = 'USER'
        else:
            params['scope'] = 'PROJECT'

        if default_variables := params.get('default_variables'):
            for widget in params.get('layouts', []):
                widget['variable'] = default_variables

        return self.project_dashboard_mgr.create_project_dashboard(params)

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'domain_id'])
    def update(self, params):
        """Update project_dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'name': 'str',
                'layouts': 'list',
                'options': 'dict',
                'default_variables': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            project_dashboard_vo (object)
        """

        project_dashboard_id = params['project_dashboard_id']
        domain_id = params['domain_id']
        layouts = params.get('layouts', [])
        options = params.get('options')
        default_variables = params.get('default_variables')

        if default_variables:
            for widget in layouts:
                widget['variable'] = default_variables

        project_dashboard_vo: ProjectDashboard = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id,
                                                                                                  domain_id)
        return self.project_dashboard_mgr.update_project_dashboard_by_vo(params, project_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'domain_id'])
    def delete(self, params):
        """Deregister project_dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """
        self.project_dashboard_mgr.delete_project_dashboard(params['project_dashboard_id'], params['domain_id'])

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'domain_id'])
    def get(self, params):
        """ Get project_dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            project_dashboard_vo (object)
        """

        project_dashboard_id = params['project_dashboard_id']
        domain_id = params['domain_id']

        return self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['domain_id'])
    @append_query_filter(['project_dashboard_id', 'name', 'labels', 'user_id', 'domain_id'])
    @append_keyword_filter(['project_dashboard_id', 'name'])
    def list(self, params):
        """ List project_dashboards

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'name': 'str',
                'scope': 'str',
                'labels': 'list',
                'user_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            project_dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})
        return self.project_dashboard_mgr.list_project_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id'])
    @append_keyword_filter(['project_dashboard_id', 'name'])
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
        return self.project_dashboard_mgr.stat_project_dashboards(query)
