import logging
import copy
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.manager.public_widget_manager import PublicWidgetManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.manager.data_table_manager.data_transformation_manager import (
    DataTransformationManager,
)
from spaceone.dashboard.manager.cost_analysis_manager import CostAnalysisManager
from spaceone.dashboard.model.public_data_table.request import *
from spaceone.dashboard.model.public_data_table.response import *
from spaceone.dashboard.model.public_data_table.database import PublicDataTable

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PublicDataTableService(BaseService):
    resource = "PublicDataTable"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pub_data_table_mgr = PublicDataTableManager()

    @transaction(
        permission="dashboard:PublicDataTable.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def add(
        self, params: PublicDataTableAddRequest
    ) -> Union[PublicDataTableResponse, dict]:
        """Add public data table

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'name': 'str',
                'source_type': 'str',           # required
                'options': 'dict',              # required
                'vars': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicDataTableResponse:
        """

        pub_data_table_info = self.add_data_table(params.dict())
        return PublicDataTableResponse(**pub_data_table_info)

    @check_required(["widget_id", "source_type", "options", "domain_id"])
    def add_data_table(self, params_dict: dict) -> dict:
        source_type = params_dict.get("source_type")
        options = params_dict.get("options")
        vars = params_dict.get("vars")
        widget_id = params_dict.get("widget_id")
        domain_id = params_dict.get("domain_id")
        workspace_id = params_dict.get("workspace_id")
        user_projects = params_dict.get("user_projects")

        pub_widget_mgr = PublicWidgetManager()
        pub_widget_vo = pub_widget_mgr.get_public_widget(
            widget_id,
            domain_id,
            workspace_id,
            user_projects,
        )

        if source_type == "COST":
            if plugin_id := options.get("COST", {}).get("plugin_id"):
                data_source_id = self._get_data_source_id_from_plugin_id(plugin_id)
                options["COST"]["data_source_id"] = data_source_id
                params_dict["options"] = options
                del params_dict["options"]["COST"]["plugin_id"]

        ds_mgr = DataSourceManager(
            "PUBLIC",
            source_type,
            options,
            widget_id,
            domain_id,
        )

        # Load data source to verify options
        ds_mgr.load(vars=vars)

        # Get data and labels info from options
        data_info, labels_info = ds_mgr.get_data_and_labels_info()

        params_dict["data_type"] = "ADDED"
        params_dict["data_info"] = data_info
        params_dict["labels_info"] = labels_info
        params_dict["dashboard_id"] = pub_widget_vo.dashboard_id
        params_dict["resource_group"] = pub_widget_vo.resource_group
        params_dict["workspace_id"] = pub_widget_vo.workspace_id
        params_dict["project_id"] = pub_widget_vo.project_id
        params_dict["state"] = ds_mgr.state
        params_dict["error_message"] = ds_mgr.error_message

        pub_data_table_vo = self.pub_data_table_mgr.create_public_data_table(
            params_dict
        )

        return pub_data_table_vo.to_dict()

    @staticmethod
    def _get_data_source_id_from_plugin_id(plugin_id: str) -> str:
        cost_mgr = CostAnalysisManager()
        params = {
            "query": {
                "filter": [{"k": "plugin_info.plugin_id", "v": plugin_id, "o": "eq"}],
            }
        }
        data_sources_info = cost_mgr.list_data_sources(params)
        if data_sources_info.get("total_count", 0) == 0:
            raise ERROR_INVALID_PARAMETER(
                key="options.COST.plugin_id",
                reason=f"Invalid plugin_id: {plugin_id}",
            )
        data_source_info = data_sources_info.get("results")[0]
        return data_source_info.get("data_source_id")

    @transaction(
        permission="dashboard:PublicDataTable.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def transform(
        self, params: PublicDataTableTransformRequest
    ) -> Union[PublicDataTableResponse, dict]:
        """Transform public data table

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'name': 'str',
                'operator': 'str',              # required
                'options': 'dict',              # required
                'vars': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str',             # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicDataTableResponse:
        """

        pub_data_table_info = self.transform_data_table(params.dict())
        return PublicDataTableResponse(**pub_data_table_info)

    @check_required(["widget_id", "operator", "options", "domain_id"])
    def transform_data_table(self, params_dict: dict) -> dict:
        operator = params_dict.get("operator")
        options = params_dict.get("options")
        operator_options = options.get(operator, {})
        vars = params_dict.get("vars")
        widget_id = params_dict.get("widget_id")
        domain_id = params_dict.get("domain_id")
        workspace_id = params_dict.get("workspace_id")
        user_projects = params_dict.get("user_projects")

        pub_widget_mgr = PublicWidgetManager()
        pub_widget_vo = pub_widget_mgr.get_public_widget(
            widget_id,
            domain_id,
            workspace_id,
            user_projects,
        )

        dt_mgr = DataTransformationManager(
            "PUBLIC",
            operator,
            operator_options,
            widget_id,
            domain_id,
        )

        # Load data table to verify options
        dt_mgr.load(vars=vars)

        # Get data and labels info from options
        data_info, labels_info = dt_mgr.get_data_and_labels_info()

        params_dict["data_type"] = "TRANSFORMED"
        params_dict["data_info"] = data_info
        params_dict["labels_info"] = labels_info
        params_dict["dashboard_id"] = pub_widget_vo.dashboard_id
        params_dict["resource_group"] = pub_widget_vo.resource_group
        params_dict["workspace_id"] = pub_widget_vo.workspace_id
        params_dict["project_id"] = pub_widget_vo.project_id
        params_dict["state"] = dt_mgr.state
        params_dict["error_message"] = dt_mgr.error_message

        pub_data_table_vo = self.pub_data_table_mgr.create_public_data_table(
            params_dict
        )

        return pub_data_table_vo.to_dict()

    @transaction(
        permission="dashboard:PublicDataTable.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def update(
        self, params: PublicDataTableUpdateRequest
    ) -> Union[PublicDataTableResponse, dict]:
        """Update public data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'name': 'str',
                'options': 'dict',
                'vars': 'dict',
                'tags': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            PublicDataTableResponse:
        """

        pub_data_table_vo: PublicDataTable = (
            self.pub_data_table_mgr.get_public_data_table(
                params.data_table_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
            )
        )

        params_dict = params.dict(exclude_unset=True)
        vars = params_dict.get("vars")

        if options := params_dict.get("options"):
            raw_filter = copy.deepcopy(options.get("filter"))

            if pub_data_table_vo.data_type == "ADDED":
                ds_mgr = DataSourceManager(
                    "PUBLIC",
                    pub_data_table_vo.source_type,
                    options,
                    pub_data_table_vo.widget_id,
                    pub_data_table_vo.domain_id,
                )

                # Load data source to verify options
                ds_mgr.load(vars=vars)

                # Get ds_mgr state and error_message
                params_dict["state"] = ds_mgr.state
                params_dict["error_message"] = ds_mgr.error_message

                # Get data and labels info from options
                data_info, labels_info = ds_mgr.get_data_and_labels_info()
                params_dict["data_info"] = data_info
                params_dict["labels_info"] = labels_info

                # change timediff format
                if timediff := options.get("timediff"):
                    if years := timediff.get("years"):
                        options["timediff"] = {"years": years}
                    elif months := timediff.get("months"):
                        options["timediff"] = {"months": months}

                    params_dict["options"] = options
            else:
                operator = pub_data_table_vo.operator
                operator_options = options.get(operator, {})
                dt_mgr = DataTransformationManager(
                    "PUBLIC",
                    operator,
                    operator_options,
                    pub_data_table_vo.widget_id,
                    pub_data_table_vo.domain_id,
                )

                # Load data table to verify options
                dt_mgr.load(vars=vars)

                # Get dt_mgr state and error_message
                params_dict["state"] = dt_mgr.state
                params_dict["error_message"] = dt_mgr.error_message

                # Get data and labels info from options
                data_info, labels_info = dt_mgr.get_data_and_labels_info()

                params_dict = params.dict()
                params_dict["data_type"] = "TRANSFORMED"
                params_dict["data_info"] = data_info
                params_dict["labels_info"] = labels_info
                params_dict["options"] = {
                    operator: operator_options,
                }

            if raw_filter:
                params_dict["options"]["filter"] = raw_filter

        pub_data_table_vo = self.pub_data_table_mgr.update_public_data_table_by_vo(
            params_dict, pub_data_table_vo
        )

        return PublicDataTableResponse(**pub_data_table_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDataTable.write",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @convert_model
    def delete(self, params: PublicDataTableDeleteRequest) -> None:
        """Delete public data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_data_table_vo: PublicDataTable = (
            self.pub_data_table_mgr.get_public_data_table(
                params.data_table_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
            )
        )

        self.pub_data_table_mgr.delete_public_data_table_by_vo(pub_data_table_vo)

    @transaction(
        permission="dashboard:PublicDataTable.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @convert_model
    def load(self, params: PublicDataTableLoadRequest) -> dict:
        """Load public data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'granularity': 'str',           # required
                'start': 'str',
                'end': 'str',
                'sort': 'list',
                'page': 'dict',
                'vars': 'dict',
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list'         # injected from auth
            }

        Returns:
            None
        """

        pub_data_table_vo: PublicDataTable = (
            self.pub_data_table_mgr.get_public_data_table(
                params.data_table_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
            )
        )

        if pub_data_table_vo.data_type == "ADDED":
            ds_mgr = DataSourceManager(
                "PUBLIC",
                pub_data_table_vo.source_type,
                pub_data_table_vo.options,
                pub_data_table_vo.widget_id,
                pub_data_table_vo.domain_id,
            )
            ds_mgr.load(
                params.granularity,
                params.start,
                params.end,
                params.vars,
            )
            return ds_mgr.response(params.sort, params.page)

        else:
            operator = pub_data_table_vo.operator
            options = pub_data_table_vo.options.get(operator, {})
            widget_id = pub_data_table_vo.widget_id
            domain_id = pub_data_table_vo.domain_id

            dt_mgr = DataTransformationManager(
                "PUBLIC",
                operator,
                options,
                widget_id,
                domain_id,
            )
            dt_mgr.load(
                params.granularity,
                params.start,
                params.end,
                params.vars,
            )
            return dt_mgr.response(params.sort, params.page)

    @transaction(
        permission="dashboard:PublicDataTable.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @convert_model
    def get(
        self, params: PublicDataTableGetRequest
    ) -> Union[PublicDataTableResponse, dict]:
        """Get public data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'workspace_id': 'str',          # injected from auth
                'domain_id': 'str'              # injected from auth (required)
                'user_projects': 'list',        # injected from auth
            }

        Returns:
            PublicDataTableResponse:
        """

        pub_data_table_vo: PublicDataTable = (
            self.pub_data_table_mgr.get_public_data_table(
                params.data_table_id,
                params.domain_id,
                params.workspace_id,
                params.user_projects,
            )
        )

        return PublicDataTableResponse(**pub_data_table_vo.to_dict())

    @transaction(
        permission="dashboard:PublicDataTable.read",
        role_types=["DOMAIN_ADMIN", "WORKSPACE_OWNER", "WORKSPACE_MEMBER"],
    )
    @change_value_by_rule("APPEND", "workspace_id", "*")
    @change_value_by_rule("APPEND", "user_projects", "*")
    @append_query_filter(
        [
            "widget_id",
            "data_table_id",
            "name",
            "state",
            "data_type",
            "source_type",
            "operator",
            "domain_id",
            "workspace_id",
            "project_id",
            "user_projects",
        ]
    )
    @append_keyword_filter(["data_table_id", "name"])
    @convert_model
    def list(
        self, params: PublicDataTableSearchQueryRequest
    ) -> Union[PublicDataTablesResponse, dict]:
        """List public data tables

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'widget_id': 'str',                             # required
                'data_table_id': 'str',
                'name': 'str',
                'data_type': 'str',
                'source_type': 'str',
                'operator': 'str',
                'project_id': 'str',
                'workspace_id': 'str',                          # injected from auth
                'domain_id': 'str',                             # injected from auth (required)
                'user_projects': 'list',                        # injected from auth
            }

        Returns:
            PublicDataTablesResponse:
        """

        query = params.query or {}
        (
            pub_data_table_vos,
            total_count,
        ) = self.pub_data_table_mgr.list_public_data_tables(query)
        pub_data_tables_info = [
            pub_data_table_vo.to_dict() for pub_data_table_vo in pub_data_table_vos
        ]
        return PublicDataTablesResponse(
            results=pub_data_tables_info, total_count=total_count
        )
