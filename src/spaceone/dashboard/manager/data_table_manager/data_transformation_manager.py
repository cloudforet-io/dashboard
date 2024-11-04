import logging
from typing import List, Union, Tuple

import numpy as np
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
    ERROR_REQUIRED_PARAMETER,
    ERROR_DUPLICATED_DATA_FIELDS,
    ERROR_NO_FIELDS_TO_JOIN,
    ERROR_INVALID_PARAMETER_TYPE,
)
from spaceone.dashboard.manager.data_table_manager import DataTableManager, GRANULARITY
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.manager.private_data_table_manager import (
    PrivateDataTableManager,
)
from spaceone.dashboard.model.public_data_table.database import PublicDataTable
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable

_LOGGER = logging.getLogger(__name__)


class DataTransformationManager(DataTableManager):
    def __init__(
        self,
        data_table_type: str,
        operator: str,
        options: dict,
        widget_id: str,
        domain_id: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if operator not in ["JOIN", "CONCAT", "AGGREGATE", "QUERY", "EVAL"]:
            raise ERROR_NOT_SUPPORTED_OPERATOR(operator=operator)

        self.data_table_type = data_table_type
        self.operator = operator
        self.options = options
        self.widget_id = widget_id
        self.domain_id = domain_id
        self.data_table_vos = self._get_data_table_from_options(operator, options)
        self.data_keys = []
        self.label_keys = []

    def get_data_and_labels_info(self) -> Tuple[dict, dict]:
        data_info = {}
        labels_info = {}
        for data_table_vo in self.data_table_vos:
            for key, value in data_table_vo.data_info.items():
                if key not in data_info and key in self.data_keys:
                    data_info[key] = value

            for key, value in data_table_vo.labels_info.items():
                if key not in labels_info and key in self.label_keys:
                    labels_info[key] = value

        for key in self.data_keys:
            if key not in data_info:
                data_info[key] = {}

        for key in self.label_keys:
            if key not in labels_info:
                labels_info[key] = {}

        return data_info, labels_info

    def load(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
        try:
            if self.operator == "JOIN":
                self.join_data_tables(granularity, start, end, vars)
            elif self.operator == "CONCAT":
                self.concat_data_tables(granularity, start, end, vars)
            elif self.operator == "AGGREGATE":
                self.aggregate_data_table(granularity, start, end, vars)
            elif self.operator == "QUERY":
                self.query_data_table(granularity, start, end, vars)
            elif self.operator == "EVAL":
                self.evaluate_data_table(granularity, start, end, vars)
        except Exception as e:
            self.error_message = e.message if hasattr(e, "message") else str(e)
            self.state = "UNAVAILABLE"
            _LOGGER.error(f"[load] {self.operator} operation error: {e}")

        return self.df

    def join_data_tables(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        how = self.options.get("how", "left")
        if how not in ["left", "right", "inner", "outer"]:
            raise ERROR_INVALID_PARAMETER(
                key="options.JOIN.how", reason="Invalid join type"
            )

        origin_vo = self.data_table_vos[0]
        other_vo = self.data_table_vos[1]
        origin_data_keys = set(origin_vo.data_info.keys())
        other_data_keys = set(other_vo.data_info.keys())

        duplicate_keys = list(origin_data_keys & other_data_keys)
        if len(duplicate_keys) > 0:
            raise ERROR_DUPLICATED_DATA_FIELDS(field=", ".join(duplicate_keys))

        self.data_keys = list(origin_data_keys | other_data_keys)
        origin_label_keys = set(origin_vo.labels_info.keys())
        other_label_keys = set(other_vo.labels_info.keys())
        self.label_keys = list(origin_label_keys | other_label_keys)

        on = list(origin_label_keys & other_label_keys)
        if len(on) == 0:
            raise ERROR_NO_FIELDS_TO_JOIN()

        fill_na = {}
        for key in self.data_keys:
            fill_na[key] = 0

        for key in self.label_keys:
            fill_na[key] = ""

        origin_df = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_df = self._get_data_table(other_vo, granularity, start, end, vars)

        if len(other_df) == 0:
            if how in ["left", "outer"]:
                self.df = origin_df
            else:
                self.df = other_df
            return
        elif len(origin_df) == 0:
            if how in ["right", "outer"]:
                self.df = other_df
            else:
                self.df = origin_df
            return

        merged_df = origin_df.merge(other_df, left_on=on, right_on=on, how=how)
        merged_df = merged_df.fillna(value=fill_na)
        self.df = merged_df

    def concat_data_tables(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        origin_vo = self.data_table_vos[0]
        other_vo = self.data_table_vos[1]
        origin_data_keys = set(origin_vo.data_info.keys())
        other_data_keys = set(other_vo.data_info.keys())

        self.data_keys = list(origin_data_keys | other_data_keys)
        origin_label_keys = set(origin_vo.labels_info.keys())
        other_label_keys = set(other_vo.labels_info.keys())
        self.label_keys = list(origin_label_keys | other_label_keys)

        fill_na = {}
        for key in self.data_keys:
            fill_na[key] = 0

        for key in self.label_keys:
            fill_na[key] = ""

        origin_df = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_df = self._get_data_table(other_vo, granularity, start, end, vars)

        merged_df = pd.concat([origin_df, other_df])
        merged_df = merged_df.fillna(value=fill_na)
        self.df = merged_df

    def aggregate_data_table(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        function = dict(self.options.get("function", {}))
        if function == {}:
            raise ERROR_REQUIRED_PARAMETER(key="options.AGGREGATE.function")

        for key, operator in function.items():
            if operator not in ["sum", "mean", "max", "min", "count"]:
                raise ERROR_INVALID_PARAMETER(
                    key="options.AGGREGATE.function",
                    reason=f"Invalid function type: {operator}",
                )

        group_by = list(self.options.get("group_by", []))

        origin_vo = self.data_table_vos[0]
        self.data_keys = list(function.keys())
        self.label_keys = group_by

        df = self._get_data_table(origin_vo, granularity, start, end, vars)

        for key in group_by:
            if key not in df.columns:
                raise ERROR_INVALID_PARAMETER(
                    key="options.AGGREGATE.group_by", reason=f"Invalid key: {key}"
                )

        for key in function.keys():
            if key not in df.columns:
                raise ERROR_INVALID_PARAMETER(
                    key="options.AGGREGATE.function", reason=f"Invalid key: {key}"
                )

        if len(group_by) > 0:
            self.df = df.groupby(group_by).agg(function).reset_index()
        else:
            self.df = df.agg(function).to_frame().T

    def query_data_table(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        conditions = self.options.get("conditions", [])

        origin_vo = self.data_table_vos[0]
        self.data_keys = list(origin_vo.data_info.keys())
        self.label_keys = list(origin_vo.labels_info.keys())

        df = self._get_data_table(origin_vo, granularity, start, end, vars)

        for condition in conditions:
            if self.is_jinja_expression(condition):
                condition, gv_type_map = self.change_global_variables(condition, vars)
                condition = self.remove_jinja_braces(condition)
                condition = self.change_expression_data_type(condition, gv_type_map)

            try:
                df = df.query(condition)
            except Exception as e:
                _LOGGER.error(f"[query_data_table] query error: {e}")
                raise ERROR_INVALID_PARAMETER(
                    key="options.QUERY.conditions", reason=condition
                )

        self.df = df

    def evaluate_data_table(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        expressions = self.options.get("expressions", [])

        origin_vo = self.data_table_vos[0]
        self.data_keys = list(origin_vo.data_info.keys())
        self.label_keys = list(origin_vo.labels_info.keys())

        df = self._get_data_table(origin_vo, granularity, start, end, vars)

        for expression in expressions:
            if isinstance(expression, dict):
                name = expression.get("name")
                field_type = expression.get("field_type", "DATA")
                condition = expression.get("condition")
                else_condition = expression.get("else")
                value_expression = expression.get("expression")

                if name is None:
                    raise ERROR_REQUIRED_PARAMETER(key="options.EVAL.expressions.name")

                if value_expression is None:
                    raise ERROR_REQUIRED_PARAMETER(
                        key="options.EVAL.expressions.expression"
                    )

                if self.is_jinja_expression(condition):
                    condition, gv_type_map = self.change_global_variables(
                        condition, vars
                    )
                    condition = self.remove_jinja_braces(condition)
                    condition = self.change_expression_data_type(condition, gv_type_map)

                if self.is_jinja_expression(value_expression):
                    value_expression, gv_type_map = self.change_global_variables(
                        value_expression, vars
                    )
                    value_expression = self.remove_jinja_braces(value_expression)
                    value_expression = self.change_expression_data_type(
                        value_expression, gv_type_map
                    )

                if self.is_jinja_expression(else_condition):
                    else_condition, gv_type_map = self.change_global_variables(
                        else_condition, vars
                    )
                    else_condition = self.remove_jinja_braces(else_condition)
                    else_condition = self.change_expression_data_type(
                        else_condition, gv_type_map
                    )

                template_vars = {}
                for key in self.data_keys:
                    template_vars[key] = f"`{key}`"

                for key in self.label_keys:
                    template_vars[key] = f"`{key}`"

                try:
                    value_expression = value_expression.format(**template_vars)
                except Exception as e:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions.expression",
                        reason=f"Invalid expression: (template_var={e})",
                    )

                if field_type not in ["DATA", "LABEL"]:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions.field_type",
                        reason=f"Invalid field type: {field_type}",
                    )

                if "@" in value_expression:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions",
                        reason="It should not have '@' symbol.",
                    )

                try:
                    merged_expr = f"`{name}` = {value_expression}"

                    if field_type == "LABEL":
                        self.label_keys = list(set(self.label_keys) | {name})
                    else:
                        self.data_keys = list(set(self.data_keys) | {name})

                    last_key = df.eval(merged_expr).columns[-1:][0]

                    if not df.empty:
                        if condition:
                            df.loc[df.query(condition).index, last_key] = df.eval(
                                merged_expr
                            )
                            if else_condition:
                                else_index = list(
                                    set(df.index) - set(df.query(condition).index)
                                )
                                df.loc[else_index, last_key] = df.eval(else_condition)

                        else:
                            df.eval(merged_expr, inplace=True)

                    if last_key.startswith("BACKTICK_QUOTED_STRING"):
                        df.rename(columns={last_key: name}, inplace=True)

                    df.replace([np.inf, -np.inf], 0, inplace=True)

                except Exception as e:
                    _LOGGER.error(f"[evaluate_data_table] eval error: {e}")
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions", reason=expression
                    )

            elif isinstance(expression, str):

                if "@" in expression:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions",
                        reason="It should not have '@' symbol.",
                    )

                try:
                    name, value_expression = expression.split("=", 1)
                    name = name.replace("`", "").strip()
                    value_expression = value_expression.strip()
                    expression = f"`{name}` = {value_expression}"

                    self.data_keys = list(set(self.data_keys) | {name})

                    df.eval(expression, inplace=True)
                    last_key = df.columns[-1:][0]
                    if last_key.startswith("BACKTICK_QUOTED_STRING"):
                        df.rename(columns={last_key: name}, inplace=True)
                    df.replace([np.inf, -np.inf], 0, inplace=True)

                except Exception as e:
                    _LOGGER.error(f"[evaluate_data_table] eval error: {e}")
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions", reason=expression
                    )
            else:
                raise ERROR_INVALID_PARAMETER_TYPE(
                    key="options.EVAL.expressions", type=type(expression)
                )

        self.df = df

    def _get_data_table(
        self,
        data_table_vo: Union[PublicDataTable, PrivateDataTable],
        granularity: GRANULARITY,
        start: str,
        end: str,
        vars: dict,
    ) -> pd.DataFrame:
        if data_table_vo.data_type == "ADDED":
            ds_mgr = DataSourceManager(
                self.data_table_type,
                data_table_vo.source_type,
                data_table_vo.options,
                data_table_vo.widget_id,
                data_table_vo.domain_id,
            )
            return ds_mgr.load(granularity, start, end, vars)
        else:
            operator = data_table_vo.operator
            options = data_table_vo.options.get(operator, {})
            ds_mgr = DataTransformationManager(
                self.data_table_type,
                data_table_vo.operator,
                options,
                data_table_vo.widget_id,
                data_table_vo.domain_id,
            )
            return ds_mgr.load(granularity, start, end, vars)

    def _get_data_table_from_options(
        self,
        operator: str,
        options: dict,
    ) -> List[Union[PublicDataTable, PrivateDataTable]]:
        parent_dt_vos = []

        if self.data_table_type == "PUBLIC":
            data_table_mgr = PublicDataTableManager()
        else:
            data_table_mgr = PrivateDataTableManager()

        if operator in ["JOIN", "CONCAT"]:
            if data_tables := options.get("data_tables"):
                if len(data_tables) != 2:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.data_tables",
                        reason="It should have 2 data tables.",
                    )

                for data_table_id in data_tables:
                    if self.data_table_type == "PUBLIC":
                        parent_dt_vo = data_table_mgr.get_public_data_table(
                            data_table_id,
                            self.domain_id,
                        )
                    else:
                        parent_dt_vo = data_table_mgr.get_private_data_table(
                            data_table_id,
                            self.domain_id,
                        )

                    if parent_dt_vo.widget_id != self.widget_id:
                        raise ERROR_INVALID_PARAMETER(
                            key="options.data_tables",
                            reason="It should have same widget_id.",
                        )
                    parent_dt_vos.append(parent_dt_vo)

            else:
                raise ERROR_REQUIRED_PARAMETER(key=f"options.{operator}.data_tables")
        else:
            if data_table_id := options.get("data_table_id"):
                if self.data_table_type == "PUBLIC":
                    parent_dt_vo = data_table_mgr.get_public_data_table(
                        data_table_id,
                        self.domain_id,
                    )
                else:
                    parent_dt_vo = data_table_mgr.get_private_data_table(
                        data_table_id,
                        self.domain_id,
                    )

                if parent_dt_vo.widget_id != self.widget_id:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.data_table_id",
                        reason="It should have same widget_id.",
                    )
                parent_dt_vos.append(parent_dt_vo)
            else:
                raise ERROR_REQUIRED_PARAMETER(key="options.data_table_id")

        return parent_dt_vos
