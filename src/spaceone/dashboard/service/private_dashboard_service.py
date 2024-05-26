import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.private_dashboard_manager import PrivateDashboardManager
from spaceone.dashboard.manager.identity_manager import IdentityManager
from spaceone.dashboard.model.private_dashboard.request import *
from spaceone.dashboard.model.private_dashboard.response import *
from spaceone.dashboard.model.private_dashboard.database import PrivateDashboard

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
    def create(self, params: PrivateDashboardCreateRequest) -> PrivateDashboardResponse:
        """Create private dashboard

        Args:
            params (dict): {
                'name': 'str',                  # required
                'layouts': 'list',
                'vars': 'dict',
                'settings': 'dict',
                'variables': 'dict',
                'variables_schema': 'dict',
                'labels': 'list',
                'tags': 'dict',
                'workspace_id': 'str',          # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo = self.pri_dashboard_mgr.create_private_dashboard(params.dict())
        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def update(self, params: PrivateDashboardUpdateRequest) -> PrivateDashboardResponse:
        """Update private dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'name': 'str',
                'layouts': 'list',
                'vars': 'dict',
                'settings': 'dict',
                'variables': 'dict',
                'variables_schema': 'list',
                'labels': 'list',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo: PrivateDashboard = self.pri_dashboard_mgr.get_private_dashboard(
            params.private_dashboard_id, params.domain_id, params.user_id
        )

        pri_dashboard_vo = self.pri_dashboard_mgr.update_private_dashboard_by_vo(
            params.dict(exclude_unset=True), pri_dashboard_vo)

        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDashboard.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def delete(self, params: PrivateDashboardDeleteRequest) -> None:
        """Delete private dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        pri_dashboard_vo: PrivateDashboard = self.pri_dashboard_mgr.get_private_dashboard(
            params.private_dashboard_id, params.domain_id, params.user_id
        )

        self.pri_dashboard_mgr.delete_private_dashboard_by_vo(pri_dashboard_vo)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    def get(self, params: PrivateDashboardGetRequest) -> PrivateDashboardResponse:
        """Get private dashboard

        Args:
            params (dict): {
                'private_dashboard_id': 'str',   # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDashboardResponse:
        """

        pri_dashboard_vo: PrivateDashboard = self.pri_dashboard_mgr.get_private_dashboard(
            params.private_dashboard_id, params.domain_id, params.user_id
        )

        return PrivateDashboardResponse(**pri_dashboard_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        ["private_dashboard_id", "name", "domain_id", "workspace_id", "user_id"]
    )
    @append_keyword_filter(["private_dashboard_id", "name"])
    def list(self, params: PrivateDashboardSearchQueryRequest) -> Union[PrivateDashboardsResponse, dict]:
        """List private dashboards

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'private_dashboard_id': 'str',
                'name': 'str',
                'user_id': 'str',                               # injected from auth (required)
                'workspace_id': 'str',
                'domain_id': 'str',                             # injected from auth (required)
            }

        Returns:
            PrivateDashboardsResponse:
        """

        query = params.query or {}
        pri_dashboard_vos, total_count = self.pri_dashboard_mgr.list_private_dashboards(query)
        pri_dashboards_info = [pri_dashboard_vo.to_dict() for pri_dashboard_vo in pri_dashboard_vos]
        return PrivateDashboardsResponse(results=pri_dashboards_info, total_count=total_count)

    @transaction(
        permission="dashboard:PrivateDashboard.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(["domain_id", "user_id"])
    @append_keyword_filter(["private_dashboard_id", "name"])
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
