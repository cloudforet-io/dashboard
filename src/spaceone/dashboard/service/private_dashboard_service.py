import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.private_dashboard_manager import PrivateDashboardManager
from spaceone.dashboard.manager.private_folder_manager import PrivateFolderManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.private_dashboard.request import *
from spaceone.dashboard.model.private_dashboard.response import *
from spaceone.dashboard.model.private_dashboard.database import PrivateDashboard
from spaceone.dashboard.service.private_widget_service import PrivateWidgetService

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PrivateDashboardService(BaseService):
    resource = "PrivateDashboard"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pri_dashboard_mgr = PrivateDashboardManager()
        self.identity_mgr = IdentityManager()

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["USER"],
    )
    @convert_model
    def create(
        self, params: PrivateDashboardCreateRequest
    ) -> Union[PrivateDashboardResponse, dict]:
        """Create private dashboard

        Args:
            params (dict): {
                'name': 'str',                  # required
                'description': 'str',
                'layouts': 'list',
                'vars': 'dict',
                'vars_schema': 'dict',
                'options': 'dict',
                'variables': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'folder_id': 'str',
                'workspace_id': 'str',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo = self.create_dashboard(params.dict())
        return PrivateDashboardResponse(**pri_dashboard_vo)

    @check_required(["name", "domain_id", "user_id"])
    def create_dashboard(self, params_dict: dict) -> dict:
        domain_id = params_dict["domain_id"]
        user_id = params_dict["user_id"]
        workspace_id = params_dict.get("workspace_id")

        layouts = params_dict.get("layouts")
        if layouts:
            del params_dict["layouts"]

        if folder_id := params_dict.get("folder_id"):
            pri_folder_mgr = PrivateFolderManager()
            pri_folder_mgr.get_private_folder(folder_id, domain_id, user_id)

        if workspace_id:
            self.identity_mgr.check_workspace(workspace_id, domain_id)

        pri_dashboard_vo = self.pri_dashboard_mgr.create_private_dashboard(params_dict)

        if layouts:
            pri_widget_svc = PrivateWidgetService()
            created_layouts = []
            for layout in layouts:
                widgets = layout.get("widgets", [])
                created_layout = {"name": layout.get("name", ""), "widgets": []}
                for widget in widgets:
                    widget["state"] = "ACTIVE"
                    widget["dashboard_id"] = pri_dashboard_vo.dashboard_id
                    widget["domain_id"] = domain_id
                    widget["user_id"] = user_id
                    widget["is_bulk"] = True

                    widget_info = pri_widget_svc.create_widget(widget)
                    created_widget_id = widget_info["widget_id"]

                    _LOGGER.debug(
                        f"[create_dashboard] create widget: {created_widget_id}"
                    )
                    created_layout["widgets"].append(created_widget_id)

                created_layouts.append(created_layout)

            self.pri_dashboard_mgr.update_private_dashboard_by_vo(
                {"layouts": created_layouts}, pri_dashboard_vo
            )

        return pri_dashboard_vo.to_dict()

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["USER"],
    )
    @convert_model
    def update(
        self, params: PrivateDashboardUpdateRequest
    ) -> Union[PrivateDashboardResponse, dict]:
        """Update private dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'name': 'str',
                'description': 'str',
                'layouts': 'list',
                'vars': 'dict',
                'vars_schema': 'dict',
                'options': 'dict',
                'variables': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'folder_id': 'str',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo: PrivateDashboard = (
            self.pri_dashboard_mgr.get_private_dashboard(
                params.dashboard_id, params.domain_id, params.user_id
            )
        )

        if params.folder_id:
            pri_folder_mgr = PrivateFolderManager()
            pri_folder_mgr.get_private_folder(
                params.folder_id, params.domain_id, params.user_id
            )

        pri_dashboard_vo = self.pri_dashboard_mgr.update_private_dashboard_by_vo(
            params.dict(exclude_unset=True), pri_dashboard_vo
        )

        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(permission="dashboard:PrivateDashboard.write", role_types=["USER"])
    @convert_model
    def change_folder(
        self, params: PrivateDashboardChangeFolderRequest
    ) -> Union[PrivateDashboardResponse, dict]:
        """Change private dashboard's folder

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'folder_id': 'str',      # required
                'user_id': 'str',        # injected from auth (required)
                'domain_id': 'str'       # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        folder_id = params.folder_id
        pri_dashboard_vo = self.pri_dashboard_mgr.get_private_dashboard(
            params.dashboard_id, params.domain_id, params.user_id
        )

        if folder_id:
            pri_folder_mgr = PrivateFolderManager()
            pri_folder_mgr.get_private_folder(
                params.folder_id, params.domain_id, params.user_id
            )

        pri_dashboard_vo = self.pri_dashboard_mgr.update_private_dashboard_by_vo(
            params.dict(), pri_dashboard_vo
        )

        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["USER"],
    )
    @convert_model
    def delete(self, params: PrivateDashboardDeleteRequest) -> None:
        """Delete private dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        pri_dashboard_vo = self.pri_dashboard_mgr.get_private_dashboard(
            params.dashboard_id, params.domain_id, params.user_id
        )

        self.pri_dashboard_mgr.delete_private_dashboard_by_vo(pri_dashboard_vo)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["USER"],
    )
    @convert_model
    def get(
        self, params: PrivateDashboardGetRequest
    ) -> Union[PrivateDashboardResponse, dict]:
        """Get private dashboard

        Args:
            params (dict): {
                'dashboard_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo = self.pri_dashboard_mgr.get_private_dashboard(
            params.dashboard_id, params.domain_id, params.user_id
        )

        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["USER"],
    )
    @append_query_filter(
        ["dashboard_id", "name", "domain_id", "workspace_id", "user_id", "folder_id"]
    )
    @append_keyword_filter(["dashboard_id", "name"])
    @convert_model
    def list(
        self, params: PrivateDashboardSearchQueryRequest
    ) -> Union[PrivateDashboardsResponse, dict]:
        """List private dashboards

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'dashboard_id': 'str',
                'name': 'str',
                'folder_id': 'str',
                'user_id': 'str',                               # injected from auth (required)
                'workspace_id': 'str',
                'domain_id': 'str',                             # injected from auth (required)
            }

        Returns:
            PrivateDashboardsResponse:
        """

        query = params.query or {}
        pri_dashboard_vos, total_count = self.pri_dashboard_mgr.list_private_dashboards(
            query
        )
        pri_dashboards_info = [
            pri_dashboard_vo.to_dict() for pri_dashboard_vo in pri_dashboard_vos
        ]
        return PrivateDashboardsResponse(
            results=pri_dashboards_info, total_count=total_count
        )

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["USER"],
    )
    @append_query_filter(["domain_id", "user_id"])
    @append_keyword_filter(["dashboard_id", "name"])
    @convert_model
    def stat(self, params: PrivateDashboardStatQueryRequest) -> dict:
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
        return self.pri_dashboard_mgr.stat_private_dashboards(query)
