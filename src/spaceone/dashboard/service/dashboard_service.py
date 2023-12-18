import copy
import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import DashboardManager, DashboardVersionManager
from spaceone.dashboard.model import Dashboard, DashboardVersion
from spaceone.dashboard.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DashboardService(BaseService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_mgr: DashboardManager = self.locator.get_manager(
            "DashboardManager"
        )
        self.version_mgr: DashboardVersionManager = self.locator.get_manager(
            "DashboardVersionManager"
        )

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["name", "dashboard_type", "user_id", "domain_id"])
    def create(self, params: dict) -> Dashboard:
        """Register domain_dashboard

        Args:
            params (dict): {
                'name': 'str',                # required
                'dashboard_type': 'str',      # required
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'resource_group': 'str',      # required
                'user_id': 'str',             # injected from auth
                'project_id': 'str',          # injected from auth
                'workspace_id': 'str',        # injected from auth
                'domain_id': 'str'            # injected from auth
            }

        Returns:
            dashboard_vo (object)
        """

        dashboard_vo = self.dashboard_mgr.create_domain_dashboard(params)

        version_keys = ["layouts", "variables", "variables_schema"]
        if any(set(version_keys) & set(params.keys())):
            self.version_mgr.create_version_by_domain_dashboard_vo(dashboard_vo, params)

        return dashboard_vo

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "domain_id"])
    def update(self, params):
        """Update domain_dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
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
            dashboard_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]

        dashboard_vo: Dashboard = self.dashboard_mgr.get_domain_dashboard(
            dashboard_id, domain_id
        )

        if "name" not in params:
            params["name"] = dashboard_vo.name

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        if "settings" in params:
            params["settings"] = self._merge_settings(
                dashboard_vo.settings, params["settings"]
            )

        version_change_keys = ["layouts", "variables", "variables_schema"]
        if self._has_version_key_in_params(dashboard_vo, params, version_change_keys):
            self.dashboard_mgr.increase_version(dashboard_vo)
            self.version_mgr.create_version_by_domain_dashboard_vo(dashboard_vo, params)

        return self.dashboard_mgr.update_domain_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "domain_id"])
    def delete(self, params):
        """Deregister domain_dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        dashboard_vo: Dashboard = self.dashboard_mgr.get_domain_dashboard(
            params["dashboard_id"], params["domain_id"]
        )

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        if domain_dashboard_version_vos := self.version_mgr.filter_versions(
            dashboard_id=dashboard_vo.dashboard_id
        ):
            self.version_mgr.delete_versions_by_domain_dashboard_version_vos(
                domain_dashboard_version_vos
            )

        self.dashboard_mgr.delete_by_domain_dashboard_vo(dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "domain_id"])
    def get(self, params):
        """Get domain_dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            dashboard_vo (object)
        """
        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]

        dashboard_vo = self.dashboard_mgr.get_domain_dashboard(
            dashboard_id, domain_id, params.get("only")
        )

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        return dashboard_vo

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "version", "domain_id"])
    def delete_version(self, params):
        """delete version of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            None
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]

        dashboard_vo = self.dashboard_mgr.get_domain_dashboard(dashboard_id, domain_id)

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        current_version = dashboard_vo.version
        if current_version == version:
            raise ERROR_LATEST_VERSION(version=version)

        self.version_mgr.delete_version(dashboard_id, version, domain_id)

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "version", "domain_id"])
    def revert_version(self, params):
        """Revert version of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
            }

        Returns:
            dashboard_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]

        dashboard_vo: Dashboard = self.dashboard_mgr.get_domain_dashboard(
            dashboard_id, domain_id
        )

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        version_vo: DashboardVersion = self.version_mgr.get_version(
            dashboard_id, version, domain_id
        )

        params["layouts"] = version_vo.layouts
        params["variables"] = version_vo.variables
        params["variables_schema"] = version_vo.variables_schema

        self.dashboard_mgr.increase_version(dashboard_vo)
        self.version_mgr.create_version_by_domain_dashboard_vo(dashboard_vo, params)

        return self.dashboard_mgr.update_domain_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "version", "domain_id"])
    def get_version(self, params):
        """Get version of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'only': 'list
            }

        Returns:
            domain_dashboard_version_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]

        dashboard_vo: Dashboard = self.dashboard_mgr.get_domain_dashboard(
            dashboard_id, domain_id
        )

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        return self.version_mgr.get_version(
            dashboard_id, version, domain_id, params.get("only")
        )

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "domain_id"])
    @append_query_filter(["dashboard_id", "version", "domain_id"])
    @append_keyword_filter(["dashboard_id", "version"])
    def list_versions(self, params):
        """List versions of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',
                'version': 'int',
                'domain_id': 'str',
                'query': 'dict (spaceone.api.core.v1.Query)'
            }

        Returns:
            domain_dashboard_version_vos (object)
            total_count
        """
        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]

        query = params.get("query", {})
        domain_dashboard_version_vos, total_count = self.version_mgr.list_versions(
            query
        )
        dashboard_vo = self.dashboard_mgr.get_domain_dashboard(dashboard_id, domain_id)

        if (
            dashboard_vo.viewers == "PRIVATE"
            and dashboard_vo.user_id != self.transaction.get_meta("user_id")
        ):
            raise ERROR_PERMISSION_DENIED()

        return domain_dashboard_version_vos, total_count, dashboard_vo.version

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["domain_id"])
    @append_query_filter(["dashboard_id", "name", "viewers", "user_id", "domain_id"])
    @append_keyword_filter(["dashboard_id", "name"])
    def list(self, params):
        """List public_dashboards

        Args:
            params (dict): {
                'dashboard_id': 'str',
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

        query = params.get("query", {})

        query["filter"] = query.get("filter", [])
        query["filter"].append(
            {
                "k": "user_id",
                "v": [self.transaction.get_meta("user_id"), None],
                "o": "in",
            }
        )

        return self.dashboard_mgr.list_domain_dashboards(query)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "domain_id"])
    @append_query_filter(["domain_id"])
    @append_keyword_filter(["dashboard_id", "name"])
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
        query = params.get("query", {})

        query["filter"] = query.get("filter", [])
        query["filter"].append(
            {
                "k": "user_id",
                "v": [self.transaction.get_meta("user_id"), None],
                "o": "in",
            }
        )

        return self.dashboard_mgr.stat_domain_dashboards(query)

    @staticmethod
    def _has_version_key_in_params(dashboard_vo, params, version_change_keys):
        layouts = dashboard_vo.layouts
        variables = dashboard_vo.variables
        variables_schema = dashboard_vo.variables_schema

        if any(key for key in params if key in version_change_keys):
            if layouts_from_params := params.get("layouts"):
                if layouts != layouts_from_params:
                    return True
            if options_from_params := params.get("variables"):
                if variables != options_from_params:
                    return True
            if schema_from_params := params.get("variables_schema"):
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
