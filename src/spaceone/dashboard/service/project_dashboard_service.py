import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import ProjectDashboardManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ProjectDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_mgr: ProjectDashboardManager = self.locator.get_manager('ProjectDashboardManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required([])
    @change_date_value(['start', 'end'])
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
                'user_id': 'str', # TODO : have to delete user_id
                'domain_id': 'str'
            }

        Returns:
            project_dashboard_vo (object)
        """
        pass

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
        pass

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
        pass

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
        pass

    @transaction(append_meta={'authorization.scope': 'USER'})
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
