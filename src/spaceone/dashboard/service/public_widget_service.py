import logging
from typing import Union

from spaceone.core.service import *
from spaceone.dashboard.manager.public_widget_manager import PublicWidgetManager
from spaceone.dashboard.manager.public_dashboard_manager import PublicDashboardManager
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.manager.data_table_manager.data_transformation_manager import (
    DataTransformationManager,
)
from spaceone.dashboard.model.public_widget.request import *
from spaceone.dashboard.model.public_widget.response import *
from spaceone.dashboard.model.public_widget.database import PublicWidget
from spaceone.dashboard.error.dashboard import (
    ERROR_NOT_SUPPORTED_VERSION,
    ERROR_INVALID_PARAMETER,
)

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicWidgetService(BaseService):
    resource = "PublicWidget"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_widget_mgr = PublicWidgetManager()

    @transaction(
        permission="dashboard:PublicWidget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def create(
        self, params: PublicWidgetCreateRequest
    ) -> Union[PublicWidgetResponse, dict]:
        """Create public widget

        Args:
            params (dict): {
                'dashboard_id': 'str',          # required
                'name': 'str',
                'description': 'str',
                'widget_type': 'str',
                'size': 'str',
                'options': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicWidgetResponse:
        """

        pub_dashboard_mgr = PublicDashboardManager()
        pub_dashboard_vo = pub_dashboard_mgr.get_public_dashboard(
            params.dashboard_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        if pub_dashboard_vo.version == "1.0":
            raise ERROR_NOT_SUPPORTED_VERSION(version=pub_dashboard_vo.version)

        pub_widget_vo = self.pub_widget_mgr.create_public_widget(params.dict())

        return PublicWidgetResponse(**pub_widget_vo.to_dict())

    @transaction(
        permission="dashboard:PublicWidget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def update(
        self, params: PublicWidgetUpdateRequest
    ) -> Union[PublicWidgetResponse, dict]:
        """Update public widget

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'name': 'str',
                'description': 'str',
                'widget_type': 'str',
                'size': 'str',
                'options': 'dict',
                'data_table_id': 'str',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicWidgetResponse:
        """

        pub_widget_vo: PublicWidget = self.pub_widget_mgr.get_public_widget(
            params.widget_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        if params.data_table_id is not None:
            pub_data_table_mgr = PublicDataTableManager()
            pub_data_table_vo = pub_data_table_mgr.get_public_data_table(
                params.data_table_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
            )

            if pub_data_table_vo.widget_id != params.widget_id:
                raise ERROR_INVALID_PARAMETER(
                    key="data_table_id",
                    reason="Data table is not belong to this widget.",
                )

        pub_widget_vo = self.pub_widget_mgr.update_public_widget_by_vo(
            params.dict(exclude_unset=True), pub_widget_vo
        )

        return PublicWidgetResponse(**pub_widget_vo.to_dict())

    @transaction(
        permission="dashboard:PublicWidget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def delete(self, params: PublicWidgetDeleteRequest) -> None:
        """Delete public widget

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_widget_vo: PublicWidget = self.pub_widget_mgr.get_public_widget(
            params.widget_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        self.pub_widget_mgr.delete_public_widget_by_vo(pub_widget_vo)

    @transaction(
        permission="dashboard:PublicWidget.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def load(self, params: PublicWidgetLoadRequest) -> dict:
        """Load public widget

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'query': 'dict (spaceone.api.core.v1.AnalyzeQuery)', # required
                'vars': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_widget_vo: PublicWidget = self.pub_widget_mgr.get_public_widget(
            params.widget_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        if pub_widget_vo.data_table_id is None:
            raise ERROR_INVALID_PARAMETER(
                key="widget_id", reason="Data table is not set."
            )

        pub_data_table_mgr = PublicDataTableManager()
        pub_data_table_vo = pub_data_table_mgr.get_public_data_table(
            pub_widget_vo.data_table_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        if pub_data_table_vo.data_type == "ADDED":
            ds_mgr = DataSourceManager(
                "PUBLIC",
                pub_data_table_vo.source_type,
                pub_data_table_vo.options,
                pub_data_table_vo.widget_id,
                pub_data_table_vo.domain_id,
            )
            return ds_mgr.load_from_widget(
                params.query,
                params.vars,
            )
        else:
            operator = pub_data_table_vo.operator
            options = pub_data_table_vo.options.get(operator, {})

            dt_mgr = DataTransformationManager(
                "PUBLIC",
                pub_data_table_vo.operator,
                options,
                pub_data_table_vo.widget_id,
                pub_data_table_vo.domain_id,
            )
            return dt_mgr.load_from_widget(
                params.query,
                params.vars,
            )

    @transaction(
        permission="dashboard:PublicWidget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def get(self, params: PublicWidgetGetRequest) -> Union[PublicWidgetResponse, dict]:
        """Get public widget

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicWidgetResponse:
        """

        pub_widget_vo: PublicWidget = self.pub_widget_mgr.get_public_widget(
            params.widget_id,
            params.domain_id,
            params.workspace_id,
            params.user_projects,
        )

        return PublicWidgetResponse(**pub_widget_vo.to_dict())

    @transaction(
        permission="dashboard:PublicWidget.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @append_query_filter(
        [
            "dashboard_id",
            "widget_id",
            "name",
            "domain_id",
            "workspace_id",
            "project_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["widget_id", "name"])
    @convert_model
    def list(
        self, params: PublicWidgetSearchQueryRequest
    ) -> Union[PublicWidgetsResponse, dict]:
        """List public widgets

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'dashboard_id': 'str',                          # required
                'widget_id': 'str',
                'name': 'str',
                'project_id': 'str',
                'workspace_id': 'str',                          # injected from auth
                'domain_id': 'str',                             # injected from auth (required)
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            PublicWidgetsResponse:
        """

        query = params.query or {}
        pub_widget_vos, total_count = self.pub_widget_mgr.list_public_widgets(query)
        pub_widgets_info = [pub_widget_vo.to_dict() for pub_widget_vo in pub_widget_vos]
        return PublicWidgetsResponse(results=pub_widgets_info, total_count=total_count)
