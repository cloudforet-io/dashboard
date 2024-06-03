import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.public_dashboard_manager import PublicDashboardManager
from spaceone.dashboard.manager.public_folder_manager import PublicFolderManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.public_dashboard.request import *
from spaceone.dashboard.model.public_dashboard.response import *
from spaceone.dashboard.model.public_dashboard.database import PublicDashboard

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

        if params.resource_group == "PROJECT":
            if not params.project_id:
                raise ERROR_REQUIRED_PARAMETER(key="project_id")

            project_info = self.identity_mgr.get_project(params.project_id)
            params.workspace_id = project_info["workspace_id"]
        elif params.resource_group == "WORKSPACE":
            if not params.workspace_id:
                raise ERROR_REQUIRED_PARAMETER(key="workspace_id")

            self.identity_mgr.check_workspace(params.workspace_id, params.domain_id)
            params.project_id = "*"
        else:
            params.workspace_id = "*"
            params.project_id = "*"

        if params.folder_id:
            pub_folder_mgr = PublicFolderManager()
            pub_folder_mgr.get_public_folder(
                params.folder_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
                params.resource_group,
            )

        pub_dashboard_vo = self.pub_dashboard_mgr.create_public_dashboard(params.dict())
        return PublicDashboardResponse(**pub_dashboard_vo.to_dict())

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
                'dashboard_id': 'str',   # required
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
