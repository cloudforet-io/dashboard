import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.public_folder_manager import PublicFolderManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.public_folder.request import *
from spaceone.dashboard.model.public_folder.response import *
from spaceone.dashboard.model.public_folder.database import PublicFolder
from spaceone.dashboard.service.public_dashboard_service import PublicDashboardService
from spaceone.dashboard.manager.public_dashboard_manager import PublicDashboardManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicFolderService(BaseService):
    resource = "PublicFolder"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_folder_mgr = PublicFolderManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="dashboard:PublicFolder.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def create(
        self, params: PublicFolderCreateRequest
    ) -> Union[PublicFolderResponse, dict]:
        """Create public folder

        Args:
            params (dict): {
                'name': 'str',                  # required
                'tags': 'dict',
                'dashboards': 'list',
                'resource_group': 'str',        # required
                'project_id': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PublicFolderResponse:
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
            params.project_id = "-"
        else:
            params.workspace_id = "-"
            params.project_id = "-"

        pub_folder_vo = self.pub_folder_mgr.create_public_folder(params.dict())

        # Create child dashboards
        if params.dashboards:
            pass

        return PublicFolderResponse(**pub_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PublicFolder.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def update(
        self, params: PublicFolderUpdateRequest
    ) -> Union[PublicFolderResponse, dict]:
        """Update public folder

        Args:
            params (dict): {
                'folder_id': 'str',             # required
                'name': 'str',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicFolderResponse:
        """

        pub_folder_vo: PublicFolder = self.pub_folder_mgr.get_public_folder(
            params.folder_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        pub_folder_vo = self.pub_folder_mgr.update_public_folder_by_vo(
            params.dict(exclude_unset=True), pub_folder_vo
        )

        return PublicFolderResponse(**pub_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PublicFolder.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def share(
        self, params: PublicFolderShareRequest
    ) -> Union[PublicFolderResponse, dict]:
        """Share public folder

        Args:
            params (dict): {
                'folder_id': 'str',             # required
                'scope': 'str',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicFolderResponse:
        """

        pub_folder_vo: PublicFolder = self.pub_folder_mgr.get_public_folder(
            params.folder_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        updated_params = {
            "shared": True,
        }

        if pub_folder_vo.resource_group == "DOMAIN":
            updated_params["workspace_id"] = "*"
            if params.scope == "PROJECT":
                updated_params["scope"] = "PROJECT"
                updated_params["project_id"] = "*"
            else:
                updated_params["scope"] = "WORKSPACE"
        elif pub_folder_vo.resource_group == "WORKSPACE":
            updated_params["project_id"] = "*"
            updated_params["scope"] = "PROJECT"
        elif pub_folder_vo.resource_group == "PROJECT":
            raise ERROR_PERMISSION_DENIED()

        pub_folder_vo = self.pub_folder_mgr.update_public_folder_by_vo(
            updated_params, pub_folder_vo
        )

        if pub_folder_vo.resource_group in ["DOMAIN", "WORKSPACE"]:
            pub_dashboard_svc = PublicDashboardService()
            pub_dashboard_mgr = PublicDashboardManager()
            pub_dashboard_vos = pub_dashboard_mgr.filter_public_dashboards(
                folder_id=pub_folder_vo.folder_id, domain_id=pub_folder_vo.domain_id
            )

            for pub_dashboard_vo in pub_dashboard_vos:
                pub_dashboard_svc.share_dashboard(
                    {
                        "dashboard_id": pub_dashboard_vo.dashboard_id,
                        "scope": params.scope,
                        "domain_id": params.domain_id,
                        "workspace_id": params.workspace_id,
                        "user_projects": params.user_projects,
                        "cascade": True,
                    }
                )

        return PublicFolderResponse(**pub_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def unshare(
        self, params: PublicFolderUnshareRequest
    ) -> Union[PublicFolderResponse, dict]:
        """Unshare public folder

        Args:
            params (dict): {
                'folder_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicDashboardResponse:
        """

        pub_folder_vo: PublicFolder = self.pub_folder_mgr.get_public_folder(
            params.folder_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        updated_params = {
            "shared": False,
        }

        if pub_folder_vo.resource_group == "DOMAIN":
            updated_params["workspace_id"] = "-"
            updated_params["project_id"] = "-"
            updated_params["scope"] = None
        elif pub_folder_vo.resource_group == "WORKSPACE":
            updated_params["project_id"] = "-"
            updated_params["scope"] = None
        elif pub_folder_vo.resource_group == "PROJECT":
            raise ERROR_PERMISSION_DENIED()

        pub_folder_vo = self.pub_folder_mgr.update_public_folder_by_vo(
            updated_params, pub_folder_vo
        )

        if pub_folder_vo.resource_group in ["DOMAIN", "WORKSPACE"]:
            pub_dashboard_svc = PublicDashboardService()
            pub_dashboard_mgr = PublicDashboardManager()
            pub_dashboard_vos = pub_dashboard_mgr.filter_public_dashboards(
                folder_id=pub_folder_vo.folder_id, domain_id=pub_folder_vo.domain_id
            )

            for pub_dashboard_vo in pub_dashboard_vos:
                pub_dashboard_svc.unshare_dashboard(
                    {
                        "dashboard_id": pub_dashboard_vo.dashboard_id,
                        "domain_id": params.domain_id,
                        "workspace_id": params.workspace_id,
                        "user_projects": params.user_projects,
                        "cascade": True,
                    }
                )

        return PublicFolderResponse(**pub_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PublicFolder.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def delete(self, params: PublicFolderDeleteRequest) -> None:
        """Delete public folder

        Args:
            params (dict): {
                'folder_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_folder_vo: PublicFolder = self.pub_folder_mgr.get_public_folder(
            params.folder_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        self.pub_folder_mgr.delete_public_folder_by_vo(pub_folder_vo)

    @transaction(
        permission="dashboard:PublicFolder.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @convert_model
    def get(self, params: PublicFolderGetRequest) -> Union[PublicFolderResponse, dict]:
        """Get public folder

        Args:
            params (dict): {
                'folder_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicFolderResponse:
        """

        pub_folder_vo: PublicFolder = self.pub_folder_mgr.get_public_folder(
            params.folder_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        return PublicFolderResponse(**pub_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PublicFolder.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(
        [
            "folder_id",
            "name",
            "domain_id",
            "workspace_id",
            "project_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["folder_id", "name"])
    @convert_model
    def list(
        self, params: PublicFolderSearchQueryRequest
    ) -> Union[PublicFoldersResponse, dict]:
        """List public folders

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'folder_id': 'str',
                'name': 'str',
                'project_id': 'str',
                'workspace_id': 'str',                          # injected from auth
                'domain_id': 'str',                             # injected from auth (required)
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            PublicFoldersResponse:
        """

        query = params.query or {}
        pub_folder_vos, total_count = self.pub_folder_mgr.list_public_folders(query)
        pub_dashboards_info = [
            pub_folder_vo.to_dict() for pub_folder_vo in pub_folder_vos
        ]
        return PublicFoldersResponse(
            results=pub_dashboards_info, total_count=total_count
        )

    @transaction(
        permission="dashboard:PublicFolder.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(["domain_id", "workspace_id", "user_projects"])
    @append_keyword_filter(["folder_id", "name"])
    @convert_model
    def stat(self, params: PublicFolderStatQueryRequest) -> dict:
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
        return self.pub_folder_mgr.stat_public_folders(query)
