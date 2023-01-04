import copy
import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import DomainDashboardManager, DomainDashboardVersionManager
from spaceone.dashboard.model import DomainDashboard, DomainDashboardVersion
from spaceone.dashboard.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DomainDashboardService(BaseService):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_dashboard_mgr: DomainDashboardManager = self.locator.get_manager('DomainDashboardManager')
        self.version_mgr: DomainDashboardVersionManager = self.locator.get_manager('DomainDashboardVersionManager')

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['name', 'viewers', 'domain_id'])
    def create(self, params):
        """Register domain_dashboard

        Args:
            params (dict): {
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
            domain_dashboard_vo (object)
        """

        if params['viewers'] == 'PUBLIC':
            params['user_id'] = None
        else:
            params['user_id'] = self.transaction.get_meta('user_id')

        domain_dashboard_vo = self.domain_dashboard_mgr.create_domain_dashboard(params)

        version_keys = ['layouts', 'variables', 'variables_schema']
        if any(set(version_keys) & set(params.keys())):
            self.version_mgr.create_version_by_domain_dashboard_vo(domain_dashboard_vo, params)

        return domain_dashboard_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_dashboard_id', 'domain_id'])
    def update(self, params):
        """Update domain_dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'name': 'str',
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str'
            }

        Returns:
            domain_dashboard_vo (object)
        """

        domain_dashboard_id = params['domain_dashboard_id']
        domain_id = params['domain_id']

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id,
                                                                                              domain_id)

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        if 'settings' in params:
            params['settings'] = self._merge_settings(domain_dashboard_vo.settings, params['settings'])

        version_change_keys = ['layouts', 'variables', 'variables_schema']
        if self._has_version_key_in_params(domain_dashboard_vo, params, version_change_keys):
            self.domain_dashboard_mgr.increase_version(domain_dashboard_vo)
            self.version_mgr.create_version_by_domain_dashboard_vo(domain_dashboard_vo, params)

        return self.domain_dashboard_mgr.update_domain_dashboard_by_vo(params, domain_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
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

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_mgr.get_domain_dashboard(
            params['domain_dashboard_id'], params['domain_id'])

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        if domain_dashboard_version_vos := self.version_mgr.filter_versions(
                domain_dashboard_id=domain_dashboard_vo.domain_dashboard_id):
            self.version_mgr.delete_versions_by_domain_dashboard_version_vos(domain_dashboard_version_vos)

        self.domain_dashboard_mgr.delete_by_domain_dashboard_vo(domain_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
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

        domain_dashboard_vo = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id, domain_id,
                                                                             params.get('only'))

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return domain_dashboard_vo

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_dashboard_id', 'version', 'domain_id'])
    def delete_version(self, params):
        """ delete version of domain dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            None
        """

        domain_dashboard_id = params['domain_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        domain_dashboard_vo = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id, domain_id)

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        current_version = domain_dashboard_vo.version
        if current_version == version:
            raise ERROR_LATEST_VERSION(version=version)

        self.version_mgr.delete_version(domain_dashboard_id, version, domain_id)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_dashboard_id', 'version', 'domain_id'])
    def revert_version(self, params):
        """ Revert version of domain dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            domain_dashboard_vo (object)
        """

        domain_dashboard_id = params['domain_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id,
                                                                                              domain_id)

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        version_vo: DomainDashboardVersion = self.version_mgr.get_version(domain_dashboard_id, version, domain_id)

        params['layouts'] = version_vo.layouts
        params['variables'] = version_vo.variables
        params['variables_schema'] = version_vo.variables_schema

        self.domain_dashboard_mgr.increase_version(domain_dashboard_vo)
        self.version_mgr.create_version_by_domain_dashboard_vo(domain_dashboard_vo, params)

        return self.domain_dashboard_mgr.update_domain_dashboard_by_vo(params, domain_dashboard_vo)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_dashboard_id', 'version', 'domain_id'])
    def get_version(self, params):
        """ Get version of domain dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            domain_dashboard_version_vo (object)
        """

        domain_dashboard_id = params['domain_dashboard_id']
        version = params['version']
        domain_id = params['domain_id']

        domain_dashboard_vo: DomainDashboard = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id,
                                                                                              domain_id)

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return self.version_mgr.get_version(domain_dashboard_id, version, domain_id, params.get('only'))

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_dashboard_id', 'domain_id'])
    @append_query_filter(['domain_dashboard_id', 'version', 'domain_id'])
    @append_keyword_filter(['domain_dashboard_id', 'version'])
    def list_versions(self, params):
        """ List versions of domain dashboard

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            domain_dashboard_version_vos (object)
            total_count
        """
        domain_dashboard_id = params['domain_dashboard_id']
        domain_id = params['domain_id']

        query = params.get('query', {})
        domain_dashboard_version_vos, total_count = self.version_mgr.list_versions(query)
        domain_dashboard_vo = self.domain_dashboard_mgr.get_domain_dashboard(domain_dashboard_id, domain_id)

        if domain_dashboard_vo.viewers == 'PRIVATE' and \
                domain_dashboard_vo.user_id != self.transaction.get_meta('user_id'):
            raise ERROR_PERMISSION_DENIED()

        return domain_dashboard_version_vos, total_count, domain_dashboard_vo.version

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
    @check_required(['domain_id'])
    @append_query_filter(['domain_dashboard_id', 'name', 'viewers', 'user_id', 'domain_id'])
    @append_keyword_filter(['domain_dashboard_id', 'name'])
    def list(self, params):
        """ List public_dashboards

        Args:
            params (dict): {
                'domain_dashboard_id': 'str',
                'name': 'str',
                'viewers': 'str',
                'user_id': 'str'
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            domain_dashboard_vos (object)
            total_count
        """

        query = params.get('query', {})

        query['filter'] = query.get('filter', [])
        query['filter'].append({
            'k': 'user_id',
            'v': [self.transaction.get_meta('user_id'), None],
            'o': 'in'
        })

        return self.domain_dashboard_mgr.list_domain_dashboards(query)

    @transaction(append_meta={'authorization.scope': 'DOMAIN'})
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

        query['filter'] = query.get('filter', [])
        query['filter'].append({
            'k': 'user_id',
            'v': [self.transaction.get_meta('user_id'), None],
            'o': 'in'
        })

        return self.domain_dashboard_mgr.stat_domain_dashboards(query)

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
