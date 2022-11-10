import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import ProjectDashboardManager, ProjectDashboardVersionManager
from spaceone.dashboard.model import ProjectDashboard, ProjectDashboardVersion
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

        project_dashboard_vo = self.project_dashboard_mgr.create_project_dashboard(params)

        version_keys = ['layouts', 'dashboard_options', 'dashboard_options_schema']
        if set(version_keys) <= params.keys():
            self.version_mgr.create_version_by_project_dashboard_vo(project_dashboard_vo)

        return project_dashboard_vo

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

        version_change_keys = ['layouts', 'dashboard_options', 'dashboard_options_schema']
        if self._check_version_change(project_dashboard_vo, params, version_change_keys):
            self.project_dashboard_mgr.increase_version(project_dashboard_vo)
            self.version_mgr.create_version_by_project_dashboard_vo(project_dashboard_vo)

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
        project_dashboard_id = params['project_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        project_dashboard_vo = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id, domain_id)
        current_version = project_dashboard_vo.version
        if current_version == version:
            raise ERROR_LATEST_VERSION(version=version)

        return self.version_mgr.delete_version(project_dashboard_id, version, domain_id)

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
        project_dashboard_id = params['project_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        project_dashboard_vo: ProjectDashboard = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id,
                                                                                                  domain_id)
        version_vo: ProjectDashboardVersion = self.version_mgr.get_version(project_dashboard_id, version, domain_id)

        params['layouts'] = version_vo.layouts
        params['dashboard_options'] = version_vo.dashboard_options
        params['dashboard_options_schema'] = version_vo.dashboard_options_schema

        return self.project_dashboard_mgr.update_project_dashboard_by_vo(params, project_dashboard_vo)

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

        return self.version_mgr.get_version(project_dashboard_id, version, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT_OR_USER'})
    @check_required(['project_dashboard_id', 'domain_id'])
    @append_query_filter(['project_dashboard_id', 'version', 'domain_id'])
    @append_keyword_filter(['project_dashboard_id', 'version'])
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
        project_dashboard_id = params['project_dashboard_id']
        domain_id = params['domain_id']

        query = params.get('query', {})
        project_dashboard_version_vos, total_count = self.version_mgr.list_versions(query)
        project_dashboard_vo = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id, domain_id)
        return project_dashboard_version_vos, total_count, project_dashboard_vo.version

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

    @staticmethod
    def _check_version_change(domain_dashboard_vo, params, version_change_keys):
        layouts = domain_dashboard_vo.layouts
        dashboard_options = domain_dashboard_vo.dashboard_options
        dashboard_options_schema = domain_dashboard_vo.dashboard_options_schema

        if any(key for key in params if key in version_change_keys):
            if layouts_from_params := params.get('layouts'):
                if layouts != layouts_from_params:
                    return True
            elif options_from_params := params.get('dashboard_options'):
                if dashboard_options != options_from_params:
                    return True
            elif schema_from_params := params.get('dashboard_options_schema'):
                if schema_from_params != dashboard_options_schema:
                    return True
            else:
                return False
