import logging
from typing import Union

from spaceone.core.service import *
from spaceone.core.error import *
from spaceone.dashboard.manager.private_data_table_manager import (
    PrivateDataTableManager,
)
from spaceone.dashboard.manager.private_widget_manager import PrivateWidgetManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.manager.data_table_manager.data_transformation_manager import (
    DataTransformationManager,
)
from spaceone.dashboard.manager.cost_analysis_manager import CostAnalysisManager
from spaceone.dashboard.model.private_data_table.request import *
from spaceone.dashboard.model.private_data_table.response import *
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@mutation_handler
@event_handler
class PrivateDataTableService(BaseService):
    resource = "PrivateDataTable"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pri_data_table_mgr = PrivateDataTableManager()

    @transaction(
        permission="dashboard:PrivateDataTable.write",
        role_types=["USER"],
    )
    @convert_model
    def add(
        self, params: PrivateDataTableAddRequest
    ) -> Union[PrivateDataTableResponse, dict]:
        """Add private data table

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'name': 'str',
                'source_type': 'str',           # required
                'options': 'dict',              # required
                'vars': 'dict',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            PrivateDataTableResponse:
        """

        pri_data_table_info = self.add_data_table(params.dict())
        return PrivateDataTableResponse(**pri_data_table_info)

    @check_required(["widget_id", "source_type", "options", "domain_id", "user_id"])
    def add_data_table(self, params_dict: dict) -> dict:
        source_type = params_dict.get("source_type")
        options = params_dict.get("options")
        vars = params_dict.get("vars")
        widget_id = params_dict.get("widget_id")
        domain_id = params_dict.get("domain_id")
        user_id = params_dict.get("user_id")

        pri_widget_mgr = PrivateWidgetManager()
        pri_widget_vo = pri_widget_mgr.get_private_widget(
            widget_id,
            domain_id,
            user_id,
        )

        if source_type == "COST":
            if plugin_id := options.get("COST", {}).get("plugin_id"):
                data_source_id = self._get_data_source_id_from_plugin_id(plugin_id)
                options["COST"]["data_source_id"] = data_source_id
                params_dict["options"] = options
                del params_dict["options"]["COST"]["plugin_id"]

        ds_mgr = DataSourceManager(
            "PRIVATE",
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
        params_dict["dashboard_id"] = pri_widget_vo.dashboard_id
        params_dict["state"] = ds_mgr.state
        params_dict["error_message"] = ds_mgr.error_message

        pri_data_table_vo = self.pri_data_table_mgr.create_private_data_table(
            params_dict
        )

        return pri_data_table_vo.to_dict()

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
        permission="dashboard:PrivateDataTable.write",
        role_types=["USER"],
    )
    @convert_model
    def transform(
        self, params: PrivateDataTableTransformRequest
    ) -> Union[PrivateDataTableResponse, dict]:
        """Add private data table

        Args:
            params (dict): {
                'widget_id': 'str',             # required
                'name': 'str',
                'operator': 'str',              # required
                'options': 'dict',              # required
                'vars': 'dict',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str',             # injected from auth (required)
            }

        Returns:
            PrivateDataTableResponse:
        """

        pri_data_table_info = self.transform_data_table(params.dict())
        return PrivateDataTableResponse(**pri_data_table_info)

    @check_required(["widget_id", "operator", "options", "domain_id", "user_id"])
    def transform_data_table(self, params_dict: dict) -> dict:
        operator = params_dict.get("operator")
        options = params_dict.get("options")
        operator_options = options.get(operator, {})
        vars = params_dict.get("vars")
        widget_id = params_dict.get("widget_id")
        domain_id = params_dict.get("domain_id")
        user_id = params_dict.get("user_id")

        pri_widget_mgr = PrivateWidgetManager()
        pri_widget_vo = pri_widget_mgr.get_private_widget(
            widget_id,
            domain_id,
            user_id,
        )

        dt_mgr = DataTransformationManager(
            "PRIVATE", operator, operator_options, widget_id, domain_id
        )

        # Load data table to verify options
        dt_mgr.load(vars=vars)

        # Get data and labels info from options
        data_info, labels_info = dt_mgr.get_data_and_labels_info()

        params_dict["data_type"] = "TRANSFORMED"
        params_dict["data_info"] = data_info
        params_dict["labels_info"] = labels_info
        params_dict["dashboard_id"] = pri_widget_vo.dashboard_id
        params_dict["state"] = dt_mgr.state
        params_dict["error_message"] = dt_mgr.error_message

        pri_data_table_vo = self.pri_data_table_mgr.create_private_data_table(
            params_dict
        )

        return pri_data_table_vo.to_dict()

    @transaction(
        permission="dashboard:PrivateDataTable.write",
        role_types=["USER"],
    )
    @convert_model
    def update(
        self, params: PrivateDataTableUpdateRequest
    ) -> Union[PrivateDataTableResponse, dict]:
        """Update private data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'name': 'str',
                'options': 'dict',
                'vars': 'dict',
                'tags': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDataTableResponse:
        """

        pri_data_table_vo: PrivateDataTable = (
            self.pri_data_table_mgr.get_private_data_table(
                params.data_table_id, params.domain_id, params.user_id
            )
        )

        params_dict = params.dict(exclude_unset=True)
        vars = params_dict.get("vars")

        if options := params_dict.get("options"):
            if pri_data_table_vo.data_type == "ADDED":
                ds_mgr = DataSourceManager(
                    "PRIVATE",
                    pri_data_table_vo.source_type,
                    options,
                    pri_data_table_vo.widget_id,
                    pri_data_table_vo.domain_id,
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
                operator = pri_data_table_vo.operator
                operator_options = options.get(operator, {})
                dt_mgr = DataTransformationManager(
                    "PRIVATE",
                    operator,
                    operator_options,
                    pri_data_table_vo.widget_id,
                    pri_data_table_vo.domain_id,
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

        pri_data_table_vo = self.pri_data_table_mgr.update_private_data_table_by_vo(
            params_dict, pri_data_table_vo
        )

        return PrivateDataTableResponse(**pri_data_table_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDataTable.write",
        role_types=["USER"],
    )
    @convert_model
    def delete(self, params: PrivateDataTableDeleteRequest) -> None:
        """Delete private data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        pri_data_table_vo: PrivateDataTable = (
            self.pri_data_table_mgr.get_private_data_table(
                params.data_table_id,
                params.domain_id,
                params.user_id,
            )
        )

        self.pri_data_table_mgr.delete_private_data_table_by_vo(pri_data_table_vo)

    @transaction(
        permission="dashboard:PrivateDataTable.read",
        role_types=["USER"],
    )
    @convert_model
    def load(self, params: PrivateDataTableLoadRequest) -> dict:
        """Load private data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'granularity': 'str',           # required
                'start': 'str',
                'end': 'str',
                'sort': 'list',
                'page': 'dict',
                'vars': 'dict',
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            None
        """

        pri_data_table_vo: PrivateDataTable = (
            self.pri_data_table_mgr.get_private_data_table(
                params.data_table_id,
                params.domain_id,
                params.user_id,
            )
        )

        if pri_data_table_vo.data_type == "ADDED":
            ds_mgr = DataSourceManager(
                "PRIVATE",
                pri_data_table_vo.source_type,
                pri_data_table_vo.options,
                pri_data_table_vo.widget_id,
                pri_data_table_vo.domain_id,
            )
            ds_mgr.load(
                params.granularity,
                params.start,
                params.end,
                params.vars,
            )
            return ds_mgr.response(params.sort, params.page)

        else:
            operator = pri_data_table_vo.operator
            options = pri_data_table_vo.options.get(operator, {})
            widget_id = pri_data_table_vo.widget_id
            domain_id = pri_data_table_vo.domain_id

            dt_mgr = DataTransformationManager(
                "PRIVATE",
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
        permission="dashboard:PrivateDataTable.read",
        role_types=["USER"],
    )
    @convert_model
    def get(
        self, params: PrivateDataTableGetRequest
    ) -> Union[PrivateDataTableResponse, dict]:
        """Get private data table

        Args:
            params (dict): {
                'data_table_id': 'str',         # required
                'user_id': 'str',               # injected from auth (required)
                'domain_id': 'str'              # injected from auth (required)
            }

        Returns:
            PrivateDataTableResponse:
        """

        pri_data_table_vo: PrivateDataTable = (
            self.pri_data_table_mgr.get_private_data_table(
                params.data_table_id,
                params.domain_id,
                params.user_id,
            )
        )

        return PrivateDataTableResponse(**pri_data_table_vo.to_dict())

    @transaction(
        permission="dashboard:PrivateDataTable.read",
        role_types=["USER"],
    )
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
            "user_id",
        ]
    )
    @append_keyword_filter(["data_table_id", "name"])
    @convert_model
    def list(
        self, params: PrivateDataTableSearchQueryRequest
    ) -> Union[PrivateDataTablesResponse, dict]:
        """List private data tables

        Args:
            params (dict): {
                'query': 'dict (spaceone.api.core.v1.Query)'
                'widget_id': 'str',                             # required
                'data_table_id': 'str',
                'name': 'str',
                'data_type': 'str',
                'source_type': 'str',
                'operator': 'str',
                'user_id': 'str',                               # injected from auth (required)
                'domain_id': 'str',                             # injected from auth (required)
            }

        Returns:
            PrivateDataTablesResponse:
        """

        query = params.query or {}
        (
            pri_data_table_vos,
            total_count,
        ) = self.pri_data_table_mgr.list_private_data_tables(query)
        pri_data_tables_info = [
            pri_data_table_vo.to_dict() for pri_data_table_vo in pri_data_table_vos
        ]
        return PrivateDataTablesResponse(
            results=pri_data_tables_info, total_count=total_count
        )
