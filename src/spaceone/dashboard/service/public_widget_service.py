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
    ERROR_REQUIRED_PARAMETER,
)
from spaceone.dashboard.service.public_data_table_service import PublicDataTableService

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
        self.pub_data_table_svc = PublicDataTableService()
        self.data_table_id_map = {}

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
                'state': 'str',
                'description': 'str',
                'widget_type': 'str',
                'size': 'str',
                'options': 'dict',
                'data_table_id': 'int',
                'data_tables': 'list',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicWidgetResponse:
        """

        params_dict = params.dict()
        if "data_tables" in params_dict:
            params_dict["is_bulk"] = True

        pub_widget_info = self.create_widget(params_dict)
        return PublicWidgetResponse(**pub_widget_info)

    @check_required(["dashboard_id", "domain_id"])
    def create_widget(self, params_dict: dict) -> dict:
        dashboard_id = params_dict["dashboard_id"]
        domain_id = params_dict["domain_id"]
        workspace_id = params_dict.get("workspace_id")
        user_projects = params_dict.get("user_projects")
        is_bulk = params_dict.get("is_bulk", False)
        data_table_idx = params_dict.get("data_table_id") or 0
        data_tables = params_dict.get("data_tables")
        self.data_table_id_map = {}

        pub_dashboard_mgr = PublicDashboardManager()
        pub_dashboard_vo = pub_dashboard_mgr.get_public_dashboard(
            dashboard_id, domain_id, workspace_id, user_projects
        )

        if pub_dashboard_vo.version == "1.0":
            raise ERROR_NOT_SUPPORTED_VERSION(version=pub_dashboard_vo.version)

        params_dict["resource_group"] = pub_dashboard_vo.resource_group
        params_dict["workspace_id"] = pub_dashboard_vo.workspace_id
        params_dict["project_id"] = pub_dashboard_vo.project_id

        if is_bulk and data_table_idx is not None:
            del params_dict["data_table_id"]

        pub_widget_vo = self.pub_widget_mgr.create_public_widget(params_dict)

        if is_bulk and data_tables:
            self._create_data_tables(
                data_tables,
                pub_widget_vo.widget_id,
                domain_id,
                workspace_id,
                user_projects,
            )

            widget_data_table_id = self.data_table_id_map[data_table_idx]
            self.pub_widget_mgr.update_public_widget_by_vo(
                {"data_table_id": widget_data_table_id}, pub_widget_vo
            )

        return pub_widget_vo.to_dict()

    def _create_data_tables(
        self,
        data_tables: list,
        widget_id: str,
        domain_id: str,
        workspace_id: str,
        user_projects: list,
    ):
        retry_data_tables = {}
        idx = 0
        for data_table in data_tables:
            data_table["widget_id"] = widget_id
            data_table["domain_id"] = domain_id
            data_table["workspace_id"] = workspace_id
            data_table["user_projects"] = user_projects
            data_table["is_bulk"] = True

            if data_type := data_table.get("data_type"):
                if data_type == "ADDED":
                    pub_data_table_info = self.pub_data_table_svc.add_data_table(
                        data_table
                    )
                elif data_type == "TRANSFORMED":
                    operator = data_table.get("operator")
                    if operator is None:
                        raise ERROR_REQUIRED_PARAMETER(
                            key="layouts.widgets.data_tables.operator"
                        )

                    options = data_table.get("options", {})
                    operator_options = options.get(operator, {})

                    if "data_table_id" in operator_options:
                        data_table_idx = operator_options["data_table_id"]
                        if data_table_idx in self.data_table_id_map:
                            data_table["options"][operator][
                                "data_table_id"
                            ] = self.data_table_id_map[data_table_idx]
                        else:
                            retry_data_tables[idx] = data_table
                            idx += 1
                            continue

                    if "data_tables" in operator_options:
                        data_tables = operator_options["data_tables"]
                        changed_data_tables = []
                        is_success = True
                        for data_table_idx in data_tables:
                            if data_table_idx in self.data_table_id_map:
                                changed_data_tables.append(
                                    self.data_table_id_map[data_table_idx]
                                )
                            else:
                                is_success = False
                                break
                        if is_success:
                            data_table["options"][operator][
                                "data_tables"
                            ] = changed_data_tables
                        else:
                            retry_data_tables[idx] = data_table
                            idx += 1
                            continue

                    pub_data_table_info = self.pub_data_table_svc.transform_data_table(
                        data_table
                    )
                else:
                    raise ERROR_INVALID_PARAMETER(
                        key="layouts.widgets.data_tables.data_type",
                        reason=f"Data type is not supported. (data_type = {data_type})",
                    )
            else:
                raise ERROR_REQUIRED_PARAMETER(
                    key="layouts.widgets.data_tables.data_type"
                )

            created_data_table_id = pub_data_table_info["data_table_id"]

            _LOGGER.debug(f"[create_widget] create data table: {created_data_table_id}")
            self.data_table_id_map[idx] = created_data_table_id
            idx += 1

        if retry_data_tables:
            self._retry_create_data_tables(retry_data_tables)

    def _retry_create_data_tables(self, data_tables: dict):
        retry_data_tables = {}
        for idx, data_table in data_tables.items():
            operator = data_table.get("operator")
            options = data_table.get("options", {})
            operator_options = options.get(operator, {})

            if "data_table_id" in operator_options:
                data_table_idx = operator_options["data_table_id"]
                if data_table_idx in self.data_table_id_map:
                    data_table["options"][operator][
                        "data_table_id"
                    ] = self.data_table_id_map[data_table_idx]
                else:
                    retry_data_tables[idx] = data_table
                    idx += 1
                    continue

            if "data_tables" in operator_options:
                data_tables = operator_options["data_tables"]
                changed_data_tables = []
                is_success = True
                for data_table_idx in data_tables:
                    if data_table_idx in self.data_table_id_map:
                        changed_data_tables.append(
                            self.data_table_id_map[data_table_idx]
                        )
                    else:
                        is_success = False
                        break
                if is_success:
                    data_table["options"][operator]["data_tables"] = changed_data_tables
                else:
                    retry_data_tables[idx] = data_table
                    idx += 1
                    continue

            pub_data_table_info = self.pub_data_table_svc.transform_data_table(
                data_table
            )
            created_data_table_id = pub_data_table_info["data_table_id"]

            _LOGGER.debug(f"[create_widget] create data table: {created_data_table_id}")
            self.data_table_id_map[idx] = created_data_table_id

        if retry_data_tables:
            self._retry_create_data_tables(retry_data_tables)

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
                'state': 'str',
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
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
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
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
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
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
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
