import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import ProjectDashboardManager, ProjectDashboardVersionManager
from spaceone.dashboard.model import ProjectDashboard
from spaceone.dashboard.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class ProjectDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_dashboard_mgr: ProjectDashboardManager = self.locator.get_manager('ProjectDashboardManager')
        self.version_mgr: ProjectDashboardVersionManager = self.locator.get_manager('ProjectDashboardVersionManager')

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_id', 'name', 'domain_id'])
    def create(self, params):
        """Register project_dashboard

        Args:
            params (dict): {
                'project_id': 'str',
                'name': 'str',
                'layouts': 'list',
                'dashboard_options': 'dict',
                'settings': 'dict',
                'dashboard_options_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'user_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            project_dashboard_vo (object)
        """

        if 'user_id' in params:
            user_id = params['user_id']
            tnx_user_id = self.transaction.get_meta('user_id')
            if user_id != tnx_user_id:
                raise ERROR_INVALID_USER_ID(user_id=user_id, tnx_user_id=tnx_user_id)
            else:
                params['scope'] = 'USER'
        else:
            params['scope'] = 'PROJECT'

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
                'dashboard_options': 'dict',
                'settings': 'dict',
                'dashboard_options_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            project_dashboard_vo (object)
        """

        project_dashboard_id = params['project_dashboard_id']
        domain_id = params['domain_id']

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
    @check_required(['project_dashboard_id', 'version', 'domain_id'])
    def delete_version(self, params):
        """ Delete version of project dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            None
        """
        pass

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'version', 'domain_id'])
    def revert_version(self, params):
        """ Revert version of project dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            project_dashboard_vo (object)
        """
        pass

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'version', 'domain_id'])
    def get_version(self, params):
        """ Get version of project dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            project_dashboard_version_vo (object)
        """

        project_dashboard_id = params['project_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        return self.version_mgr.get_version(project_dashboard_id, version, domain_id)

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['domain_dashboard_id', 'domain_id'])
    @append_query_filter(['domain_dashboard_id', 'version', 'domain_id'])
    @append_keyword_filter(['domain_dashboard_id', 'version'])
    def list_versions(self, params):
        """ List versions of project dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            project_dashboard_version_vos (object)
            total_count
        """
        query = params.get('query', {})
        return self.version_mgr.list_versions(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['domain_id'])
    @append_query_filter(['project_dashboard_id', 'project_id', 'name', 'scope', 'user_id', 'domain_id'])
    @append_keyword_filter(['project_dashboard_id', 'name'])
    def list(self, params):
        """ List project_dashboards

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'project_id': 'str',
                'name': 'str',
                'scope': 'str',
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
