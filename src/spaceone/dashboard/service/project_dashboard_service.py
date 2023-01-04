import copy
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

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['project_id', 'name', 'viewers', 'domain_id'])
    def create(self, params):
        """Register project_dashboard

        Args:
            params (dict): {
                'project_id': 'str',
                'name': 'str',
                'viewers': 'str',
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            project_dashboard_vo (object)
        """

        if params['viewers'] == 'PUBLIC':
            params['user_id'] = None
        else:
            params['user_id'] = self.transaction.get_meta('user_id')

        project_dashboard_vo = self.project_dashboard_mgr.create_project_dashboard(params)

        version_keys = ['layouts', 'variables', 'variables_schema']
        if any(set(version_keys) & set(params.keys())):
            self.version_mgr.create_version_by_project_dashboard_vo(project_dashboard_vo, params)

        return project_dashboard_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['project_dashboard_id', 'domain_id'])
    def update(self, params):
        """Update project_dashboard

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'name': 'str',
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'dict',
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

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        if 'settings' in params:
            params['settings'] = self._merge_settings(project_dashboard_vo.settings, params['settings'])

        version_change_keys = ['layouts', 'variables', 'variables_schema']
        if self._has_version_key_in_params(project_dashboard_vo, params, version_change_keys):
            self.project_dashboard_mgr.increase_version(project_dashboard_vo)
            self.version_mgr.create_version_by_project_dashboard_vo(project_dashboard_vo, params)

        return self.project_dashboard_mgr.update_project_dashboard_by_vo(params, project_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        project_dashboard_vo: ProjectDashboard = self.project_dashboard_mgr.get_project_dashboard(
            params['project_dashboard_id'], params['domain_id'])

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        if project_dashboard_version_vos := self.version_mgr.filter_versions(
                project_dashboard_id=project_dashboard_vo.project_dashboard_id):
            self.version_mgr.delete_versions_by_project_dashboard_version_vos(project_dashboard_version_vos)

        self.project_dashboard_mgr.delete_by_project_dashboard_vo(project_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        project_dashboard_vo = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id, domain_id,
                                                                                params.get('only'))

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return project_dashboard_vo

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        current_version = project_dashboard_vo.version
        if current_version == version:
            raise ERROR_LATEST_VERSION(version=version)

        self.version_mgr.delete_version(project_dashboard_id, version, domain_id)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        version_vo: ProjectDashboardVersion = self.version_mgr.get_version(project_dashboard_id, version, domain_id)

        params['layouts'] = version_vo.layouts
        params['variables'] = version_vo.variables
        params['variables_schema'] = version_vo.variables_schema

        self.project_dashboard_mgr.increase_version(project_dashboard_vo)
        self.version_mgr.create_version_by_project_dashboard_vo(project_dashboard_vo, params)

        return self.project_dashboard_mgr.update_project_dashboard_by_vo(params, project_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        project_dashboard_vo: ProjectDashboard = self.project_dashboard_mgr.get_project_dashboard(project_dashboard_id,
                                                                                                  domain_id)

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return self.version_mgr.get_version(project_dashboard_id, version, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
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

        if project_dashboard_vo.viewers == 'PRIVATE' and \
                project_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return project_dashboard_version_vos, total_count, project_dashboard_vo.version

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['domain_id'])
    @append_query_filter(
        ['project_dashboard_id', 'project_id', 'name', 'viewers', 'user_id', 'domain_id', 'user_projects'])
    @append_keyword_filter(['project_dashboard_id', 'name'])
    def list(self, params):
        """ List project_dashboards

        Args:
            params (dict): {
                'project_dashboard_id': 'str',
                'project_id': 'str',
                'name': 'str',
                'viewers': 'str',
                'user_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)',
                'user_projects': 'list', // from meta
            }

        Returns:
            project_dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})

        query['filter'] = query.get('filter', [])
        query['filter'].append({
            'k': 'user_id',
            'v': [self.transaction.get_meta('user_id'), None],
            'o': 'in'
        })

        return self.project_dashboard_mgr.list_project_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'PROJECT'})
    @check_required(['query', 'domain_id'])
    @append_query_filter(['domain_id', 'user_projects'])
    @append_keyword_filter(['project_dashboard_id', 'name'])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)',
                'user_projects': 'list', // from meta
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        query = params.get('query', {})

        query['filter'] = query.get('filter', [])
        query['filter'].append({
            'k': 'user_id',
            'v': [self.transaction.get_meta('user_id'), None],
            'o': 'in'
        })

        return self.project_dashboard_mgr.stat_project_dashboards(query)

    @staticmethod
    def _has_version_key_in_params(domain_dashboard_vo, params, version_change_keys):
        layouts = domain_dashboard_vo.layouts
        variables = domain_dashboard_vo.variables
        variables_schema = domain_dashboard_vo.variables_schema

        if any(key for key in params if key in version_change_keys):
            if layouts_from_params := params.get('layouts'):
                if layouts != layouts_from_params:
                    return True
            if options_from_params := params.get('variables'):
                if variables != options_from_params:
                    return True
            if schema_from_params := params.get('variables_schema'):
                if schema_from_params != variables_schema:
                    return True
            return False

    @staticmethod
    def _merge_settings(old_settings, new_settings):
        settings = copy.deepcopy(old_settings)

        if old_settings:
            settings.update(new_settings)
            return settings
        else:
            return new_settings
