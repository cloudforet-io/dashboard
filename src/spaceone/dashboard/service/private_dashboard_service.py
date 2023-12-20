import copy
import logging

from spaceone.core.service import *
from spaceone.dashboard.manager import (
    PrivateDashboardManager,
    PrivateDashboardVersionManager,
    IdentityManager,
)
from spaceone.dashboard.model import PrivateDashboard, PrivateDashboardVersion
from spaceone.dashboard.error import *

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PrivateDashboardService(BaseService):
    resource = "PrivateDashboard"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_mgr: PrivateDashboardManager = self.locator.get_manager(
            "PrivateDashboardManager"
        )
        self.version_mgr: PrivateDashboardVersionManager = self.locator.get_manager(
            "PrivateDashboardVersionManager"
        )
        self.identity_mgr: IdentityManager = self.locator.get_manager("IdentityManager")

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["name", "workspace_id", "domain_id"])
    def create(self, params: dict) -> PrivateDashboard:
        """Register private_dashboard

        Args:
            params (dict): {
                'name': 'str',                 # required
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str',            # injected from auth (required)
                'workspace_id': 'str',         # injected from auth (required)
            }

        Returns:
            private_dashboard_vo (object)
        """

        params["user_id"] = self.transaction.get_meta("authorization.user_id")

        dashboard_vo = self.dashboard_mgr.create_private_dashboard(params)

        version_keys = ["layouts", "variables", "variables_schema"]
        if any(set(version_keys) & set(params.keys())):
            self.version_mgr.create_version_by_private_dashboard_vo(
                dashboard_vo, params
            )

        return dashboard_vo

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "workspace_id", "domain_id"])
    def update(self, params):
        """Update private_dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'name': 'str',
                'layouts': 'list',
                'variables': 'dict',
                'settings': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'domain_id': 'str',              # injected from auth (required)
                'workspace_id': 'str',           # injected from auth (required)
            }

        Returns:
            private_dashboard_vo (object)
        """

        private_dashboard_id = params["private_dashboard_id"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        dashboard_vo: PrivateDashboard = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        if "name" not in params:
            params["name"] = dashboard_vo.name

        if "settings" in params:
            params["settings"] = self._merge_settings(
                dashboard_vo.settings, params["settings"]
            )

        version_change_keys = ["layouts", "variables", "variables_schema"]
        if self._has_version_key_in_params(dashboard_vo, params, version_change_keys):
            self.dashboard_mgr.increase_version(dashboard_vo)
            self.version_mgr.create_version_by_private_dashboard_vo(
                dashboard_vo, params
            )

        return self.dashboard_mgr.update_private_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "workspace_id", "domain_id"])
    def delete(self, params):
        """Deregister private_dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',  # required
                'domain_id': 'str',             # injected from auth (required)
                'workspace_id': 'str',          # injected from auth (required)
            }

        Returns:
            None
        """
        private_dashboard_id = params["private_dashboard_id"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        dashboard_vo: PrivateDashboard = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        if private_dashboard_version_vos := self.version_mgr.filter_versions(
            private_dashboard_id=dashboard_vo.private_dashboard_id, domain_id=domain_id
        ):
            self.version_mgr.delete_versions_by_private_dashboard_version_vos(
                private_dashboard_version_vos
            )

        self.dashboard_mgr.delete_by_private_dashboard_vo(dashboard_vo)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "workspace_id", "domain_id"])
    def get(self, params):
        """Get private_dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'domain_id': 'str',              # injected from auth (required)
                'workspace_id': 'str',           # injected from auth (required)
            }

        Returns:
            private_dashboard_vo (object)
        """
        private_dashboard_id = params["private_dashboard_id"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        dashboard_vo = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        return dashboard_vo

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "version", "workspace_id", "domain_id"])
    def delete_version(self, params):
        """delete version of domain dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'version': 'int',                # required
                'domain_id': 'str',              # injected from auth (required)
                'workspace_id': 'str',           # injected from auth (required)
                'user_id': 'str'                 # injected from auth (required)
            }

        Returns:
            None
        """

        private_dashboard_id = params["private_dashboard_id"]
        version = params["version"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        dashboard_vo = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        current_version = dashboard_vo.version
        if current_version == version:
            raise ERROR_LATEST_VERSION(version=version)

        self.version_mgr.delete_version(private_dashboard_id, version, domain_id)

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "version", "workspace_id", "domain_id"])
    def revert_version(self, params):
        """Revert version of domain dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',    # required
                'version': 'int',                 # required
                'domain_id': 'str',               # injected from auth (required)
                'domain_id': 'str',               # injected from auth (required)
                'workspace_id': 'str',            # injected from auth (required)
            }

        Returns:
            private_dashboard_vo (object)
        """

        private_dashboard_id = params["private_dashboard_id"]
        version = params["version"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        dashboard_vo: PrivateDashboard = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        version_vo: PrivateDashboardVersion = self.version_mgr.get_version(
            private_dashboard_id, version, domain_id
        )

        params["layouts"] = version_vo.layouts
        params["variables"] = version_vo.variables
        params["variables_schema"] = version_vo.variables_schema

        self.dashboard_mgr.increase_version(dashboard_vo)
        self.version_mgr.create_version_by_private_dashboard_vo(dashboard_vo, params)

        return self.dashboard_mgr.update_private_dashboard_by_vo(params, dashboard_vo)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "version", "workspace_id", "domain_id"])
    def get_version(self, params):
        """Get version of domain dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'version': 'int',                # required
                'domain_id': 'str',              # injected from auth (required)
                'workspace_id': 'str',           # injected from auth (required)
            }

        Returns:
            private_dashboard_version_vo (object)
        """

        private_dashboard_id = params["private_dashboard_id"]
        version = params["version"]
        domain_id = params["domain_id"]

        return self.version_mgr.get_version(private_dashboard_id, version, domain_id)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["private_dashboard_id", "workspace_id", "domain_id"])
    @append_query_filter(["private_dashboard_id", "version", "domain_id"])
    @append_keyword_filter(["private_dashboard_id", "version"])
    def list_versions(self, params):
        """List versions of domain dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',                   # required
                'query': 'dict (spaceone.api.core.v1.Query)'
                'version': 'int',
                'domain_id': 'str',                              # injected from auth (required)
                'workspace_id': 'str',                           # injected from auth (required)
            }

        Returns:
            private_dashboard_version_vos (object)
            total_count
        """
        private_dashboard_id = params["private_dashboard_id"]
        workspace_id = params["workspace_id"]
        domain_id = params["domain_id"]

        query = params.get("query", {})
        private_dashboard_version_vos, total_count = self.version_mgr.list_versions(
            query
        )
        dashboard_vo = self.dashboard_mgr.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )

        return private_dashboard_version_vos, total_count, dashboard_vo.version

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["workspace_id", "domain_id"])
    @append_query_filter(["private_dashboard_id", "name", "domain_id", "workspace_id"])
    @append_keyword_filter(["private_dashboard_id", "name"])
    def list(self, params):
        """List private_dashboards

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'private_dashboard_id': 'str',
                'name': 'str',
                'domain_id': 'str',                           # injected from auth (required)
                'workspace_id': 'str',                        # injected from auth (required)
            }

        Returns:
            private_dashboard_vos (object)
            total_count
        """

        user_id = self.transaction.get_meta("authorization.user_id")

        query = params.get("query", {})
        query["filter"].append({"k": "user_id", "v": user_id, "o": "eq"})

        return self.dashboard_mgr.list_private_dashboards(query)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @check_required(["query", "workspace_id", "domain_id"])
    @append_query_filter(["domain_id", "workspace_id"])
    @append_keyword_filter(["private_dashboard_id"])
    def stat(self, params):
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'domain_id': 'str',                                      # injected from auth (required)
                'workspace_id': 'str',                                   # injected from auth (required)
            }

        Returns:
            values (list) : 'list of statistics data'

        """
        user_id = self.transaction.get_meta("authorization.user_id")

        query = params.get("query", {})
        query["filter"].append({"k": "user_id", "v": user_id, "o": "eq"})

        return self.dashboard_mgr.stat_private_dashboards(query)

    @staticmethod
    def _has_version_key_in_params(
        dashboard_vo: PrivateDashboard, params: dict, version_change_keys: list
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
