import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.private_folder_manager import PrivateFolderManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.private_folder.request import *
from spaceone.dashboard.model.private_folder.response import *
from spaceone.dashboard.model.private_folder.database import PrivateFolder

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PrivateFolderService(BaseService):
    resource = "PrivateFolder"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pri_folder_mgr = PrivateFolderManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="dashboard:PrivateFolder.write",
        role_types=["USER"],
    )
    @convert_model
    def create(
        self, params: PrivateFolderCreateRequest
    ) -> Union[PrivateFolderResponse, dict]:
        """Create private folder

        Args:
            params (dict): {
                'name': 'str',                  # required
                'tags': 'dict',
                'dashboards': 'list',
                'workspace_id': 'str',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateFolderResponse:
        """

        if params.workspace_id:
            self.identity_mgr.check_workspace(params.workspace_id, params.domain_id)

        pri_folder_vo = self.pri_folder_mgr.create_private_folder(params.dict())

        # Create child dashboards
        if params.dashboards:
            pass

        return PrivateFolderResponse(**pri_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateFolder.write",
        role_types=["USER"],
    )
    @convert_model
    def update(
        self, params: PrivateFolderUpdateRequest
    ) -> Union[PrivateFolderResponse, dict]:
        """Update private folder

        Args:
            params (dict): {
                'folder_id': 'str',   # required
                'name': 'str',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateFolderResponse:
        """

        pri_folder_vo = self.pri_folder_mgr.get_private_folder(
            params.folder_id, params.domain_id, params.user_id
        )

        pri_folder_vo = self.pri_folder_mgr.update_private_folder_by_vo(
            params.dict(exclude_unset=True), pri_folder_vo
        )

        return PrivateFolderResponse(**pri_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateFolder.write",
        role_types=["USER"],
    )
    @convert_model
    def delete(self, params: PrivateFolderDeleteRequest) -> None:
        """Delete private folder

        Args:
            params (dict): {
                'folder_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        pri_folder_vo = self.pri_folder_mgr.get_private_folder(
            params.folder_id, params.domain_id, params.user_id
        )

        self.pri_folder_mgr.delete_private_folder_by_vo(pri_folder_vo)

    @transaction(
        permission="dashboard:PrivateFolder.read",
        role_types=["USER"],
    )
    @convert_model
    def get(
        self, params: PrivateFolderGetRequest
    ) -> Union[PrivateFolderResponse, dict]:
        """Get private folder

        Args:
            params (dict): {
                'folder_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateFolderResponse:
        """

        pri_folder_vo = self.pri_folder_mgr.get_private_folder(
            params.folder_id, params.domain_id, params.user_id
        )

        return PrivateFolderResponse(**pri_folder_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateFolder.read",
        role_types=["USER"],
    )
    @append_query_filter(["folder_id", "name", "domain_id", "workspace_id", "user_id"])
    @append_keyword_filter(["folder_id", "name"])
    @convert_model
    def list(
        self, params: PrivateFolderSearchQueryRequest
    ) -> Union[PrivateFoldersResponse, dict]:
        """List private folders

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'folder_id': 'str',
                'name': 'str',
                'user_id': 'str',                               # injected from auth (required)
                'workspace_id': 'str',
                'domain_id': 'str',                             # injected from auth (required)
            }

        Returns:
            PrivateFoldersResponse:
        """

        query = params.query or {}
        pri_folder_vos, total_count = self.pri_folder_mgr.list_private_folders(query)
        pri_dashboards_info = [
            pri_folder_vo.to_dict() for pri_folder_vo in pri_folder_vos
        ]
        return PrivateFoldersResponse(
            results=pri_dashboards_info, total_count=total_count
        )

    @transaction(
        permission="dashboard:PrivateFolder.read",
        role_types=["USER"],
    )
    @append_query_filter(["domain_id", "user_id"])
    @append_keyword_filter(["folder_id", "name"])
    @convert_model
    def stat(self, params: PrivateFolderStatQueryRequest) -> dict:
        """
        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.StatisticsQuery)'
                'user_id': 'str',                                       # injected from auth (required)
                'domain_id': 'str'                                      # injected from auth (required)
            }

        Returns:
            dict:

        """

        query = params.query or {}
        return self.pri_folder_mgr.stat_private_folders(query)
