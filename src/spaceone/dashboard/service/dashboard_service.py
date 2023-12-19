import copy
import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import (
    DashboardManager,
    DashboardVersionManager,
    IdentityManager,
)
from spaceone.dashboard.model import Dashboard, DashboardVersion
from spaceone.dashboard.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class DashboardService(BaseService):
    resource = "Dashboard"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_mgr: DashboardManager = self.locator.get_manager(
            "DashboardManager"
        )
        self.version_mgr: DashboardVersionManager = self.locator.get_manager(
            "DashboardVersionManager"
        )
        self.identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["name", "dashboard_type", "domain_id"])
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
                'project_id': 'str',
                'workspace_id': 'str',        # injected from auth
                'domain_id': 'str'            # injected from auth (required)
            }

        Returns:
            dashboard_vo (object)
        """

        resource_group = params["resource_group"]
        project_id = params.get("project_id")
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]

        # Check permission by resource group
        if resource_group == "PROJECT":
            project_info = self.identity_mgr.get_project(project_id)
            params["workspace_id"] = project_info["workspace_id"]
        elif resource_group == "WORKSPACE":
            self.identity_mgr.check_workspace(workspace_id, domain_id)
            params["project_id"] = "*"
        else:
            params["workspace_id"] = "*"
            params["project_id"] = "*"

        dashboard_vo = self.dashboard_mgr.create_dashboard(params)

        version_keys = ["layouts", "variables", "variables_schema"]
        if any(set(version_keys) & set(params.keys())):
            self.version_mgr.create_version_by_dashboard_vo(dashboard_vo, params)

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
                'dashboard_id': 'str',          # required
                'name': 'str',
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            dashboard_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if "name" not in params:
            params["name"] = dashboard_vo.name

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
            raise ERROR_PERMISSION_DENIED()

        if "settings" in params:
            params["settings"] = self._merge_settings(
                dashboard_vo.settings, params["settings"]
            )

        version_change_keys = ["layouts", "variables", "variables_schema"]
        if self._has_version_key_in_params(dashboard_vo, params, version_change_keys):
            self.dashboard_mgr.increase_version(dashboard_vo)
            self.version_mgr.create_version_by_dashboard_vo(dashboard_vo, params)

        return self.dashboard_mgr.update_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["dashboard_id", "domain_id"])
    def delete(self, params):
        """Deregister domain_dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',       # required
                'workspace_id': 'str',       # injected from auth
                'domain_id': 'str'           # injected from auth (required)
                'user_projects': 'list'      # injected from auth
            }

        Returns:
            None
        """
        dashboard_id = params["dashboard_id"]
        workspace_id = params.get("workspace_id")
        domain_id = params["domain_id"]
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
            raise ERROR_PERMISSION_DENIED()

        if domain_dashboard_version_vos := self.version_mgr.filter_versions(
            dashboard_id=dashboard_vo.dashboard_id, domain_id=domain_id
        ):
            self.version_mgr.delete_versions_by_dashboard_version_vos(
                domain_dashboard_version_vos
            )

        self.dashboard_mgr.delete_by_dashboard_vo(dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["dashboard_id", "domain_id"])
    def get(self, params):
        """Get domain_dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',    # required
                'workspace_id': 'str',    # injected from auth
                'domain_id': 'str'        # injected from auth (required)
                'user_projects': 'list',  # injected from auth
            }

        Returns:
            dashboard_vo (object)
        """
        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
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
                'dashboard_id': 'str',    # required
                'version': 'int',         # required
                'workspace_id': 'str',    # injected from auth
                'domain_id': 'str'        # injected from auth (required)
                'user_projects': 'list',  # injected from auth
            }

        Returns:
            None
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
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
                'dashboard_id': 'str',    # required
                'version': 'int',         # required
                'workspace_id': 'str',    # injected from auth
                'domain_id': 'str'        # injected from auth (required)
                'user_projects': 'list',  # injected from auth
            }

        Returns:
            dashboard_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
            raise ERROR_PERMISSION_DENIED()

        version_vo: DashboardVersion = self.version_mgr.get_version(
            dashboard_id, version, domain_id
        )

        params["layouts"] = version_vo.layouts
        params["variables"] = version_vo.variables
        params["variables_schema"] = version_vo.variables_schema

        self.dashboard_mgr.increase_version(dashboard_vo)
        self.version_mgr.create_version_by_dashboard_vo(dashboard_vo, params)

        return self.dashboard_mgr.update_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["dashboard_id", "version", "domain_id"])
    def get_version(self, params):
        """Get version of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',    # required
                'version': 'int',         # required
                'user_id': 'str',         # injected from auth
                'workspace_id': 'str',    # injected from auth
                'domain_id': 'str'        # injected from auth (required)
                'user_projects': 'list',  # injected from auth
            }

        Returns:
            domain_dashboard_version_vo (object)
        """

        dashboard_id = params["dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        dashboard_vo: Dashboard = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
            raise ERROR_PERMISSION_DENIED()

        return self.version_mgr.get_version(dashboard_id, version, domain_id)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["dashboard_id", "domain_id"])
    @append_query_filter(["dashboard_id", "version", "domain_id"])
    @append_keyword_filter(["dashboard_id", "version"])
    def list_versions(self, params):
        """List versions of domain dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',                          # required
                'query': 'dict (spaceone.api.core.v1.Query)'
                'version': 'int',
                'workspace_id': 'str',                          # injected from auth
                'domain_id': 'str'                              # injected from auth (required)
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            domain_dashboard_version_vos (object)
            total_count
        """
        dashboard_id = params["dashboard_id"]
        domain_id = params["domain_id"]
        workspace_id = params.get("workspace_id")
        user_projects = params.get("user_projects")
        user_id = self.transaction.get_meta("authorization.user_id")

        query = params.get("query", {})
        domain_dashboard_version_vos, total_count = self.version_mgr.list_versions(
            query
        )
        dashboard_vo = self.dashboard_mgr.get_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if dashboard_vo.dashboard_type == "PRIVATE" and dashboard_vo.user_id != user_id:
            raise ERROR_PERMISSION_DENIED()

        return domain_dashboard_version_vos, total_count, dashboard_vo.version

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["domain_id"])
    @append_query_filter(["dashboard_id", "name", "viewers", "domain_id"])
    @append_keyword_filter(["dashboard_id", "name"])
    def list(self, params):
        """List public_dashboards

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'dashboard_id': 'str',
                'name': 'str',
                'dashboard_type': 'str',
                'workspace_id': 'str',
                'project_id': 'str',
                'domain_id': 'str',                            # injected from auth
                'user_projects': 'list',                       # injected from auth
            }

        Returns:
            domain_dashboard_vos (object)
            total_count
        """

        user_id = self.transaction.get_meta("authorization.user_id")
        query = params.get("query", {})

        query["filter"] = query.get("filter", [])
        query["filter"].append(
            {
                "k": "user_id",
                "v": [user_id, None],
                "o": "in",
            }
        )

        return self.dashboard_mgr.list_dashboards(query)

    @transaction(
        permission="dashboard:Dashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @check_required(["query", "domain_id"])
    @append_query_filter(["domain_id"])
    @append_keyword_filter(["dashboard_id", "name"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'workspace_id': 'str',                                  # injected from auth
                'domain_id': 'str'                                      # injected from auth (required)
                'user_projects': 'list',                                # injected from auth
            }

        Returns:
            values (list) : 'list of statistics data'

        """

        user_id = self.transaction.get_meta("authorization.user_id")
        query = params.get("query", {})

        query["filter"] = query.get("filter", [])
        query["filter"].append(
            {
                "k": "user_id",
                "v": [user_id, None],
                "o": "in",
            }
        )

        return self.dashboard_mgr.stat_dashboards(query)

    @staticmethod
    def _has_version_key_in_params(
        dashboard_vo: Dashboard, params: dict, version_change_keys: list
    ) -> bool:
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
    def _merge_settings(old_settings: dict, new_settings: dict) -> dict:
        settings = copy.deepcopy(old_settings)

        if old_settings:
            settings.update(new_settings)
            return settings
        else:
            return new_settings
