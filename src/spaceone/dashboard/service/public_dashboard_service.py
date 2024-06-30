import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.public_dashboard_manager import PublicDashboardManager
from spaceone.dashboard.manager.public_folder_manager import PublicFolderManager
from spaceone.dashboard.manager.public_widget_manager import PublicWidgetManager
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.public_dashboard.request import *
from spaceone.dashboard.model.public_dashboard.response import *
from spaceone.dashboard.model.public_dashboard.database import PublicDashboard
from spaceone.dashboard.service.public_widget_service import PublicWidgetService

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicDashboardService(BaseService):
    resource = "PublicDashboard"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_dashboard_mgr = PublicDashboardManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="dashboard:PublicDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def create(
        self, params: PublicDashboardCreateRequest
    ) -> Union[PublicDashboardResponse, dict]:
        """Create public dashboard

        Args:
            params (dict): {
                'name': 'str',                  # required
                'description': 'str',
                'layouts': 'list',
                'vars': 'dict',
                'options': 'dict',
                'variables': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'folder_id': 'str',
                'resource_group': 'str',        # required
                'project_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PublicDashboardResponse:
        """

        pub_dashboard_info = self.create_dashboard(params.dict())
        return PublicDashboardResponse(**pub_dashboard_info)

    @check_required(["name", "resource_group", "domain_id"])
    def create_dashboard(self, params_dict: dict) -> dict:
        resource_group = params_dict["resource_group"]
        domain_id = params_dict["domain_id"]
        workspace_id = params_dict.get("workspace_id")
        user_projects = params_dict.get("user_projects")

        layouts = params_dict.get("layouts")
        if layouts:
            del params_dict["layouts"]

        if resource_group == "PROJECT":
            if project_id := params_dict.get("project_id"):
                project_info = self.identity_mgr.get_project(project_id)
                params_dict["workspace_id"] = project_info["workspace_id"]
            else:
                raise ERROR_REQUIRED_PARAMETER(key="project_id")

        elif resource_group == "WORKSPACE":
            if workspace_id := params_dict.get("workspace_id"):
                self.identity_mgr.check_workspace(workspace_id, domain_id)
                params_dict["project_id"] = "-"
            else:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")
        else:
            params_dict["workspace_id"] = "-"
            params_dict["project_id"] = "-"

        if folder_id := params_dict.get("folder_id"):
            pub_folder_mgr = PublicFolderManager()
            pub_folder_mgr.get_public_folder(
                folder_id,
                domain_id,
                workspace_id,
                user_projects,
                resource_group,
            )

        pub_dashboard_vo = self.pub_dashboard_mgr.create_public_dashboard(params_dict)

        if layouts:
            created_layouts = self._create_layouts(
                layouts,
                pub_dashboard_vo.dashboard_id,
                domain_id,
                workspace_id,
                user_projects,
            )
            self.pub_dashboard_mgr.update_public_dashboard_by_vo(
                {"layouts": created_layouts}, pub_dashboard_vo
            )

        return pub_dashboard_vo.to_dict()

    @staticmethod
    def _create_layouts(
        layouts: list,
        dashboard_id: str,
        domain_id: str,
        workspace_id: str,
        user_projects: list,
    ) -> list:
        pub_widget_svc = PublicWidgetService()
        created_layouts = []
        for layout in layouts:
            widgets = layout.get("widgets", [])
            created_layout = {"name": layout.get("name", ""), "widgets": []}
            for widget in widgets:
                widget["dashboard_id"] = dashboard_id
                widget["domain_id"] = domain_id
                widget["workspace_id"] = workspace_id
                widget["user_projects"] = user_projects
                widget["is_bulk"] = True

                widget_info = pub_widget_svc.create_widget(widget)
                created_widget_id = widget_info["widget_id"]

                _LOGGER.debug(f"[create_dashboard] create widget: {created_widget_id}")
                created_layout["widgets"].append(created_widget_id)

            created_layouts.append(created_layout)
        return created_layouts

    @transaction(
        permission="dashboard:PublicDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def update(
        self, params: PublicDashboardUpdateRequest
    ) -> Union[PublicDashboardResponse, dict]:
        """Update public dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',          # required
                'description': 'str',
                'name': 'str',
                'layouts': 'list',
                'vars': 'dict',
                'options': 'dict',
                'variables': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'folder_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicDashboardResponse:
        """

        pub_dashboard_vo: PublicDashboard = self.pub_dashboard_mgr.get_public_dashboard(
            params.dashboard_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        if params.folder_id:
            pub_folder_mgr = PublicFolderManager()
            pub_folder_mgr.get_public_folder(
                params.folder_id,
                pub_dashboard_vo.domain_id,
                pub_dashboard_vo.workspace_id,
                params.user_projects,
                pub_dashboard_vo.resource_group,
            )

        pub_dashboard_vo = self.pub_dashboard_mgr.update_public_dashboard_by_vo(
            params.dict(exclude_unset=True), pub_dashboard_vo
        )

        return PublicDashboardResponse(**pub_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def share(
        self, params: PublicDashboardShareRequest
    ) -> Union[PublicDashboardResponse, dict]:
        """Share public dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',          # required
                'workspace': 'bool',
                'project': 'bool',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicDashboardResponse:
        """

        workspace_share = params.workspace or False
        project_share = params.project or False

        pub_dashboard_vo: PublicDashboard = self.pub_dashboard_mgr.get_public_dashboard(
            params.dashboard_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        updated_params = {
            "shared": {
                "workspace": workspace_share,
                "project": project_share,
            }
        }

        if pub_dashboard_vo.resource_group == "DOMAIN":
            if workspace_share:
                updated_params["workspace_id"] = "*"
            else:
                updated_params["workspace_id"] = "-"
            if project_share:
                updated_params["project_id"] = "*"
            else:
                updated_params["project_id"] = "-"
        if pub_dashboard_vo.resource_group == "WORKSPACE":
            if workspace_share:
                raise ERROR_PERMISSION_DENIED()
            if project_share:
                updated_params["project_id"] = "*"
            else:
                updated_params["project_id"] = "-"
        elif pub_dashboard_vo.resource_group == "PROJECT":
            if workspace_share or project_share:
                raise ERROR_PERMISSION_DENIED()

        pub_dashboard_vo = self.pub_dashboard_mgr.update_public_dashboard_by_vo(
            updated_params, pub_dashboard_vo
        )

        if workspace_share or project_share:
            # Cascade update for widgets
            pub_widget_mgr = PublicWidgetManager()
            pub_widget_vos = pub_widget_mgr.filter_public_widgets(
                dashboard_id=pub_dashboard_vo.dashboard_id,
                domain_id=pub_dashboard_vo.domain_id,
            )
            for pub_widget_vo in pub_widget_vos:
                pub_widget_mgr.update_public_widget_by_vo(updated_params, pub_widget_vo)

            # Cascade update for data tables
            pub_data_table_mgr = PublicDataTableManager()
            pub_data_table_vos = pub_data_table_mgr.filter_public_data_tables(
                dashboard_id=pub_dashboard_vo.dashboard_id,
                domain_id=pub_dashboard_vo.domain_id,
            )
            for pub_data_table_vo in pub_data_table_vos:
                pub_data_table_mgr.update_public_data_table_by_vo(
                    updated_params, pub_data_table_vo
                )

        return PublicDashboardResponse(**pub_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def delete(self, params: PublicDashboardDeleteRequest) -> None:
        """Delete public dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_dashboard_vo: PublicDashboard = self.pub_dashboard_mgr.get_public_dashboard(
            params.dashboard_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        self.pub_dashboard_mgr.delete_public_dashboard_by_vo(pub_dashboard_vo)

    @transaction(
        permission="dashboard:PublicDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @convert_model
    def get(
        self, params: PublicDashboardGetRequest
    ) -> Union[PublicDashboardResponse, dict]:
        """Get public dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicDashboardResponse:
        """

        pub_dashboard_vo: PublicDashboard = self.pub_dashboard_mgr.get_public_dashboard(
            params.dashboard_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        return PublicDashboardResponse(**pub_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(
        [
            "dashboard_id",
            "name",
            "domain_id",
            "workspace_id",
            "project_id",
            "folder_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["dashboard_id", "name"])
    @convert_model
    def list(
        self, params: PublicDashboardSearchQueryRequest
    ) -> Union[PublicDashboardsResponse, dict]:
        """List public dashboards

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'dashboard_id': 'str',
                'name': 'str',
                'folder_id': 'str',
                'project_id': 'str',
                'workspace_id': 'str',                          # injected from auth
                'domain_id': 'str',                             # injected from auth (required)
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            PublicDashboardsResponse:
        """

        query = params.query or {}
        pub_dashboard_vos, total_count = self.pub_dashboard_mgr.list_public_dashboards(
            query
        )
        pub_dashboards_info = [
            pub_dashboard_vo.to_dict() for pub_dashboard_vo in pub_dashboard_vos
        ]
        return PublicDashboardsResponse(
            results=pub_dashboards_info, total_count=total_count
        )

    @transaction(
        permission="dashboard:PublicDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(["domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["dashboard_id", "name"])
    @convert_model
    def stat(self, params: PublicDashboardStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'workspace_id': 'str',                                  # injected from auth
                'domain_id': 'str'                                      # injected from auth (required)
                'user_projects': 'list',                                # injected from auth
            }

        Returns:
            dict:

        """

        query = params.query or {}
        return self.pub_dashboard_mgr.stat_public_dashboards(query)
