import logging
import re
from typing import List, Union, Tuple

import numpy as np
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
    ERROR_REQUIRED_PARAMETER,
    ERROR_DUPLICATED_DATA_FIELDS,
    ERROR_INVALID_PARAMETER_TYPE,
    ERROR_DUPLICATED_FIELD_NAME,
    ERROR_NOT_ALLOWED_DATA_FIELD,
)
from spaceone.dashboard.manager.data_table_manager import DataTableManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.manager.private_data_table_manager import (
    PrivateDataTableManager,
)
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable
from spaceone.dashboard.model.public_data_table.database import PublicDataTable

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

        if operator not in [
            "JOIN",
            "CONCAT",
            "AGGREGATE",
            "QUERY",
            "EVAL",
            "PIVOT",
            "ADD_LABELS",
            "VALUE_MAPPING",
            "SORT",
        ]:
            raise ERROR_NOT_SUPPORTED_OPERATOR(operator=operator)

        self.data_table_type = data_table_type
        self.operator = operator
        self.options = options
        self.widget_id = widget_id
        self.domain_id = domain_id
        self.data_table_vos = self._get_data_table_from_options(operator, options)
        self.data_keys = []
        self.label_keys = []
        self.total_series = None

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
        granularity: str = "MONTHLY",
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
            elif self.operator == "PIVOT":
                self.pivot_data_table(granularity, start, end, vars)
            elif self.operator == "ADD_LABELS":
                self.add_labels_data_table(granularity, start, end, vars)
            elif self.operator == "VALUE_MAPPING":
                self.value_mapping_data_table(granularity, start, end, vars)
            elif self.operator == "SORT":
                self.sort_data_table(granularity, start, end, vars)

            self.state = "AVAILABLE"
            self.error_message = None

        except Exception as e:
            self.state = "UNAVAILABLE"
            self.error_message = e.message if hasattr(e, "message") else str(e)
            _LOGGER.error(f"[load] {self.operator} operation error: {e}", exc_info=True)

        return self.df

    def join_data_tables(
        self,
        granularity: str,
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        how = self.options.get("how", "left")
        left_keys = self.options.get("left_keys")
        right_keys = self.options.get("right_keys")

        self._validate_options(how, left_keys, right_keys)

        origin_vo = self.data_table_vos[0]
        other_vo = self.data_table_vos[1]
        origin_df = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_df = self._get_data_table(other_vo, granularity, start, end, vars)

        self._validate_join_keys(left_keys, right_keys, origin_vo, other_vo)

        self._set_data_keys(origin_vo, other_vo)

        merged_df = self._merge_data_frames(
            origin_df, other_df, how, left_keys, right_keys
        )

        merged_df = self._rename_duplicated_columns(
            merged_df, origin_vo.name, other_vo.name
        )

        self.label_keys = list(set(merged_df.columns) - set(self.data_keys))
        self.df = merged_df

    def concat_data_tables(
        self,
        granularity: str,
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

        merged_df = pd.concat([origin_df, other_df], ignore_index=True)
        merged_df = merged_df.fillna(value=fill_na)
        self.df = merged_df

    def aggregate_data_table(
        self,
        granularity: str,
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
        if df is None or df.empty:
            self.df = pd.DataFrame(columns=self.label_keys + self.data_keys)
            return

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
        granularity: str,
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
                condition = self.remove_jinja_braces(condition, gv_type_map)
                condition = self.change_expression_data_type(condition, gv_type_map)
                condition = self.change_space_variable(condition)

            df = self.apply_query(df, condition)

        self.df = df

    def evaluate_data_table(
        self,
        granularity: str,
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        expressions = self.options.get("expressions", [])

        origin_vo = self.data_table_vos[0]
        self.data_keys = list(origin_vo.data_info.keys())
        self.label_keys = list(origin_vo.labels_info.keys())

        df = self._get_data_table(origin_vo, granularity, start, end, vars)
        if df is None or df.empty:
            new_keys = [
                expression.get("name")
                for expression in expressions
                if isinstance(expression, dict) and expression.get("name")
            ]

            self.data_keys = list(set(self.data_keys) | set(new_keys))

            self.df = pd.DataFrame(columns=self.label_keys + self.data_keys)
            return

        for expression in expressions:
            if isinstance(expression, dict):
                name = expression.get("name")
                field_type = expression.get("field_type", "DATA")
                condition = expression.get("condition")
                value_expression = expression.get("expression")

                if name is None:
                    raise ERROR_REQUIRED_PARAMETER(key="options.EVAL.expressions.name")

                if name in self.data_keys:
                    raise ERROR_DUPLICATED_FIELD_NAME(field=name, fields=self.data_keys)

                if value_expression is None:
                    raise ERROR_REQUIRED_PARAMETER(
                        key="options.EVAL.expressions.expression"
                    )

                if self.is_jinja_expression(condition):
                    condition, gv_type_map = self.change_global_variables(
                        condition, vars
                    )
                    condition = self.remove_jinja_braces(condition, gv_type_map)
                    condition = self.change_expression_data_type(condition, gv_type_map)
                    condition = self.change_space_variable(condition)

                if self.is_jinja_expression(value_expression):
                    value_expression, gv_type_map = self.change_global_variables(
                        value_expression, vars
                    )
                    value_expression = self.remove_jinja_braces(
                        value_expression, gv_type_map
                    )
                    value_expression = self.change_expression_data_type(
                        value_expression, gv_type_map
                    )
                    value_expression = self.change_space_variable(value_expression)

                template_vars = {}
                for key in self.data_keys:
                    template_vars[key] = f"`{key}`"

                for key in self.label_keys:
                    template_vars[key] = f"`{key}`"

                if field_type not in ["DATA", "LABEL"]:
                    raise ERROR_INVALID_PARAMETER(
                        key="options.EVAL.expressions.field_type",
                        reason=f"Invalid field type: {field_type}",
                    )

                if isinstance(value_expression, str):
                    try:
                        value_expression = value_expression.format(**template_vars)
                    except KeyError as e:
                        value_expression = value_expression.replace("{", "{{").replace(
                            "}", "}}"
                        )
                        value_expression = value_expression.format(**template_vars)
                    except Exception as e:
                        raise ERROR_INVALID_PARAMETER(
                            key="options.EVAL.expressions.expression",
                            reason=f"Invalid expression: (template_var={e})",
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

                    if name in df.columns:
                        last_key = name
                    else:
                        last_key = df.eval(merged_expr).columns[-1:][0]

                    if not df.empty:
                        if condition:
                            matched_index = self.apply_query(df, condition).index
                            if " " in name and name in df.columns:
                                temp_key = name.replace(" ", "_")
                                df.rename(columns={name: temp_key}, inplace=True)
                                merged_expr = f"`{temp_key}` = {value_expression}"
                                df.loc[matched_index, temp_key] = df.eval(
                                    merged_expr
                                ).loc[matched_index, temp_key]
                                df.rename(columns={temp_key: name}, inplace=True)
                            else:
                                df.loc[matched_index, last_key] = df.eval(
                                    merged_expr
                                ).loc[matched_index, last_key]

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

    def pivot_data_table(
        self,
        granularity: str,
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        origin_vo = self.data_table_vos[0]
        field_options = self.options["fields"]
        labels, column, data = (
            field_options.get("labels"),
            field_options["column"],
            field_options["data"],
        )
        function = self.options.get("function", "sum")

        raw_df = self._get_data_table(origin_vo, granularity, start, end, vars)

        if raw_df.empty:
            self.df = raw_df
        else:
            self._check_columns(raw_df, labels, column, data)
            fill_value = self._set_fill_value_from_df(raw_df, data)

            pivot_table = self._create_pivot_table(
                raw_df, labels, column, data, function, fill_value
            )

            pivot_table = self._sort_and_filter_pivot_table(pivot_table)

            self.df = pivot_table

    def add_labels_data_table(
        self,
        granularity: str,
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        data_table_vo = self.data_table_vos[0]
        label_keys = list(data_table_vo.labels_info.keys())
        data_keys = list(data_table_vo.data_info.keys())
        labels = self.options.get("labels")

        df = self._get_data_table(data_table_vo, granularity, start, end, vars)

        self.validate_labels(labels, df)

        self.add_labels_to_dataframe(df, labels, label_keys, data_keys)

        df = df.reindex(columns=label_keys + data_keys)

        self.label_keys = label_keys
        self.data_keys = data_keys

        self.df = df

    def value_mapping_data_table(
        self,
        granularity: str = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        data_table_vo = self.data_table_vos[0]
        df = self._get_data_table(data_table_vo, granularity, start, end, vars)

        self.label_keys = list(data_table_vo.labels_info.keys())
        self.data_keys = list(data_table_vo.data_info.keys())

        name = self.options["name"]
        if name in self.data_keys:
            raise ERROR_NOT_ALLOWED_DATA_FIELD(
                name=name, data_fields=list(self.data_keys)
            )

        field_type = self.options.get("field_type", "LABEL")

        filtered_df = self.filter_data(df, vars)
        filtered_df = self.apply_cases(filtered_df)

        df.loc[filtered_df.index, name] = filtered_df[name]
        self.handle_unfiltered_data(df, filtered_df, name, field_type)

        self.df = df

    def sort_data_table(
        self,
        granularity: str = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        data_table_vo = self.data_table_vos[0]
        df = self._get_data_table(data_table_vo, granularity, start, end, vars)

        self.label_keys = list(data_table_vo.labels_info.keys())
        self.data_keys = list(data_table_vo.data_info.keys())

        sort_options = self.options.get("sort")
        for sort_option in sort_options:
            if sort_option["key"] not in df.columns:
                raise ERROR_INVALID_PARAMETER(
                    key="options.SORT.key",
                    reason=f"Invalid key: {sort_option['key']}",
                )

        sort_keys = [option["key"] for option in sort_options]
        ascending_list = [not option.get("desc", False) for option in sort_options]

        df = df.sort_values(by=sort_keys, ascending=ascending_list)
        self.df = df

    def _get_data_table(
        self,
        data_table_vo: Union[PublicDataTable, PrivateDataTable],
        granularity: str,
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

    def _set_data_keys(
        self,
        origin_vo: Union[PublicDataTable, PrivateDataTable],
        other_vo: Union[PublicDataTable, PrivateDataTable],
    ) -> None:
        origin_data_keys = set(origin_vo.data_info.keys())
        other_data_keys = set(other_vo.data_info.keys())

        duplicate_keys = list(origin_data_keys & other_data_keys)
        if duplicate_keys:
            raise ERROR_DUPLICATED_DATA_FIELDS(field=", ".join(duplicate_keys))

        self.data_keys = list(origin_data_keys | other_data_keys)

    @staticmethod
    def _validate_options(how: str, left_keys: list, right_keys: list) -> None:
        if how not in ["left", "right", "inner", "outer"]:
            raise ERROR_INVALID_PARAMETER(
                key="options.JOIN.how", reason="Invalid join type"
            )
        if not left_keys or not right_keys:
            missing_key = "left_keys" if not left_keys else "right_keys"
            raise ERROR_REQUIRED_PARAMETER(key=f"options.JOIN.{missing_key}")

    @staticmethod
    def _validate_join_keys(
        left_keys: list,
        right_keys: list,
        origin_vo: Union[PublicDataTable, PrivateDataTable],
        other_vo: Union[PublicDataTable, PrivateDataTable],
    ) -> None:
        origin_label_keys = set(origin_vo.labels_info.keys())
        other_label_keys = set(other_vo.labels_info.keys())

        if len(left_keys) != len(right_keys):
            raise ERROR_INVALID_PARAMETER(
                key="options.JOIN",
                reason=f"left_keys and right_keys should have the same length. "
                f"left_keys={list(origin_label_keys)}, right_keys={list(other_label_keys)}",
            )

        for key in left_keys:
            if key not in origin_label_keys:
                raise ERROR_INVALID_PARAMETER(
                    key="options.JOIN.left_keys",
                    reason=f"Invalid key: {key}, table keys={list(origin_label_keys)}",
                )
        for key in right_keys:
            if key not in other_label_keys:
                raise ERROR_INVALID_PARAMETER(
                    key="options.JOIN.right_keys",
                    reason=f"Invalid key: {key}, table keys={list(other_label_keys)}",
                )

    @staticmethod
    def _create_rename_columns_from_join_keys(left_keys, right_keys, how):
        multi_keys = zip(left_keys, right_keys)
        rename_columns = {}
        if how in ["left", "inner", "outer"]:
            for left_key, right_key in multi_keys:
                rename_columns[right_key] = left_key
        elif how == "right":
            for left_key, right_key in multi_keys:
                rename_columns[left_key] = right_key

        return rename_columns

    def _merge_data_frames(
        self,
        origin_df: pd.DataFrame,
        other_df: pd.DataFrame,
        how: str,
        left_keys: list,
        right_keys: list,
    ) -> pd.DataFrame:
        rename_columns = self._create_rename_columns_from_join_keys(
            left_keys, right_keys, how
        )
        if how in ["left", "inner", "outer"]:
            other_df = other_df.rename(columns=rename_columns)
        elif how == "right":
            origin_df = origin_df.rename(columns=rename_columns)

        if origin_df.empty:
            origin_df = pd.DataFrame(columns=left_keys)

        if other_df.empty:
            other_df = pd.DataFrame(columns=right_keys)

        join_keys = left_keys if how in ["left", "inner", "outer"] else right_keys
        merged_df = origin_df.merge(
            other_df, left_on=join_keys, right_on=join_keys, how=how
        )

        label_keys = list(set(merged_df.columns) - set(self.data_keys))
        fill_na = {key: 0 for key in self.data_keys}
        fill_na.update({key: "" for key in label_keys})
        merged_df = merged_df.fillna(value=fill_na)

        return merged_df

    @staticmethod
    def _rename_duplicated_columns(
        merged_df: pd.DataFrame, origin_vo_name: str, other_vo_name: str
    ) -> pd.DataFrame:
        merged_rename_columns = {}
        for column in merged_df.columns:
            if column.endswith("_x"):
                column_name, _ = column.split("_")
                merged_rename_columns[column] = f"{column_name}({origin_vo_name})"
            elif column.endswith("_y"):
                column_name, _ = column.split("_")
                merged_rename_columns[column] = f"{column_name}({other_vo_name})"

        return merged_df.rename(columns=merged_rename_columns)

    @staticmethod
    def _check_columns(
        df: pd.DataFrame, label_fields: list, column_field: str, data_field: str
    ) -> None:
        df_columns = set(df.columns)
        if label_fields:
            for label_field in label_fields:
                if label_field not in df_columns:
                    raise ERROR_INVALID_PARAMETER(
                        key=f"options.PIVOT.labels",
                        reason=f"Invalid key: {label_field}, columns={list(df_columns)}",
                    )

        if column_field not in df_columns:
            raise ERROR_INVALID_PARAMETER(
                key=f"options.PIVOT.column",
                reason=f"Invalid key: {column_field}, columns={list(df_columns)}",
            )

        if data_field not in df_columns:
            raise ERROR_INVALID_PARAMETER(
                key=f"options.PIVOT.data",
                reason=f"Invalid key: {data_field}, columns={list(df_columns)}",
            )

    @staticmethod
    def _set_fill_value_from_df(df: pd.DataFrame, data_field: str) -> Union[int, str]:
        if df[data_field].dtype == "object":
            return ""
        return 0

    @staticmethod
    def _set_new_column_names(pivot_table: pd.DataFrame) -> pd.DataFrame:
        if pivot_table.columns[0] == "index":
            pivot_table = pivot_table.iloc[:, 1:]
        else:
            new_columns = [
                lower_col if lower_col else upper_col
                for upper_col, lower_col in pivot_table.columns
            ]
            pivot_table.columns = new_columns
        return pivot_table

    def _set_keys(self, columns: list) -> None:
        if columns[0] == "index":
            self.label_keys = []
            self.data_keys = [col for col in columns if col != "index"]
        else:
            self.label_keys = [
                upper_col for upper_col, lower_col in columns if not lower_col
            ]
            self.data_keys = [
                lower_col for upper_col, lower_col in columns if lower_col
            ]

    @staticmethod
    def _validate_select_fields(
        select_fields: list,
        column_fields: list,
    ) -> None:
        for select_field in select_fields:
            if select_field not in column_fields:
                raise ERROR_INVALID_PARAMETER(
                    key="options.PIVOT.select",
                    reason=f"Invalid key: {select_field}, columns={column_fields}",
                )

    @staticmethod
    def _validate_function(function: str) -> None:
        if function not in ["sum", "mean", "max", "min"]:
            raise ERROR_INVALID_PARAMETER(
                key="options.PIVOT.function",
                reason=f"Invalid function type: {function}",
            )

    def _create_pivot_table(
        self,
        raw_df: pd.DataFrame,
        labels: list,
        column: str,
        data: str,
        function: str,
        fill_value: Union[int, str],
    ) -> pd.DataFrame:
        try:
            self._validate_function(function)
            pivot_table = pd.pivot_table(
                raw_df,
                values=[data],
                index=labels,
                columns=[column],
                aggfunc=function,
                fill_value=fill_value,
            )
            pivot_table.reset_index(inplace=True)
            self._set_keys(list(pivot_table.columns))
            return self._set_new_column_names(pivot_table)
        except Exception as e:
            _LOGGER.error(f"[pivot_data_table] pivot error: {e}")
            raise ERROR_INVALID_PARAMETER(key="options.PIVOT", reason=str(e))

    def _sort_and_filter_pivot_table(self, pivot_table: pd.DataFrame) -> pd.DataFrame:
        column_fields = list(set(pivot_table.columns) - set(self.label_keys))
        if not self.total_series:
            self.total_series = pivot_table[column_fields].sum(axis=1)

        pivot_table = self._apply_row_sorting(pivot_table)

        if select_fields := self.options.get("select"):
            self._validate_select_fields(select_fields, column_fields)
            pivot_table = pivot_table[self.label_keys + select_fields]
            column_fields = select_fields

        if order_by := self.options.get("order_by"):
            self._validate_order_by_type(order_by)
            pivot_table = self._apply_order_by(pivot_table, order_by, column_fields)

        if limit := self.options.get("limit"):
            if isinstance(limit, float):
                limit = int(limit)
            limited_columns = pivot_table.iloc[
                :, len(self.label_keys) : len(self.label_keys) + limit
            ]
            pivot_table = pivot_table.iloc[:, : len(self.label_keys) + limit]

            pivot_table["Others"] = self.total_series.loc[
                pivot_table.index
            ] - limited_columns.sum(axis=1)

        pivot_table["Sub Total"] = self.total_series.loc[pivot_table.index]
        self.data_keys = [
            col for col in pivot_table.columns if col not in set(self.label_keys)
        ]

        return pivot_table

    @staticmethod
    def validate_labels(labels: dict, df: pd.DataFrame) -> None:
        if not labels:
            raise ERROR_REQUIRED_PARAMETER(key="options.ADD_LABELS.labels")

        for label_key in labels.keys():
            if label_key in df.columns:
                raise ERROR_INVALID_PARAMETER(
                    key="options.ADD_LABELS.labels",
                    reason=f"Duplicated key: {label_key}, columns={list(df.columns)}",
                )

    @staticmethod
    def update_keys(
        key: str,
        value,
        label_keys: list,
        data_keys: list,
    ) -> None:
        if isinstance(value, str):
            label_keys.append(key)
        elif isinstance(value, (int, float)):
            data_keys.append(key)
        else:
            raise ERROR_INVALID_PARAMETER_TYPE(
                key="options.ADD_LABELS.labels", type=type(value)
            )

    def add_labels_to_dataframe(
        self,
        df: pd.DataFrame,
        labels: dict,
        label_keys: list,
        data_keys: list,
    ) -> None:
        for key, value in labels.items():
            df[key] = value
            self.update_keys(key, value, label_keys, data_keys)

    def filter_data(self, df: pd.DataFrame, vars: dict) -> pd.DataFrame:
        condition = self.options.get("condition")
        if not condition:
            return df.copy()

        if self.is_jinja_expression(condition):
            condition, gv_type_map = self.change_global_variables(condition, vars)
            condition = self.remove_jinja_braces(condition, gv_type_map)
            condition = self.change_expression_data_type(condition, gv_type_map)
            condition = self.change_space_variable(condition)

        return self.apply_query(df, condition)

    def apply_cases(self, filtered_df: pd.DataFrame) -> pd.DataFrame:
        name = self.options["name"]
        key = self.options["key"]
        else_value = self.options.get("else")
        cases = self.options.get("cases", [])

        if key not in filtered_df.columns:
            if name not in filtered_df.columns:
                filtered_df[name] = pd.NA
            return filtered_df

        if name not in filtered_df.columns:
            filtered_df[name] = pd.NA

        if not filtered_df.empty:
            temp_key_column = f"_{key}_backup"
            if temp_key_column not in filtered_df.columns:
                filtered_df[temp_key_column] = filtered_df[key]

            for case in cases:
                self._validate_case(case)
                operator = case["operator"]
                value = case["value"]
                match = case["match"].strip()

                if operator == "eq":
                    condition = (filtered_df[key] == match) & (filtered_df[name].isna())
                    filtered_df.loc[condition, name] = value

                elif operator == "regex":
                    condition = (filtered_df[key].str.contains(match, na=False)) & (
                        filtered_df[name].isna()
                    )
                    filtered_df.loc[condition, name] = value

            if else_value is not None:
                filtered_df.loc[filtered_df[name].isna(), name] = else_value
            else:
                if name in filtered_df.columns:
                    filtered_df.loc[filtered_df[name].isna(), name] = filtered_df[
                        temp_key_column
                    ]

            filtered_df.drop(columns=[temp_key_column], inplace=True)

        return filtered_df

    def handle_unfiltered_data(
        self,
        df: pd.DataFrame,
        filtered_df: pd.DataFrame,
        name: str,
        field_type: str,
    ):
        unfiltered_index = df.index.difference(filtered_df.index)

        if field_type == "LABEL":
            df.loc[unfiltered_index, name] = ""
            self.label_keys.append(name)
        elif field_type == "DATA":
            df.loc[unfiltered_index, name] = 0
            self.data_keys.append(name)

    def _apply_row_sorting(
        self,
        pivot_table: pd.DataFrame,
    ) -> pd.DataFrame:
        pivot_table["_total"] = self.total_series
        pivot_table = pivot_table.sort_values(by="_total", ascending=False)
        pivot_table = pivot_table.drop(columns=["_total"])
        return pivot_table

    @staticmethod
    def _validate_order_by_type(order_by: dict) -> None:
        order_by_type = order_by.get("type", "key")
        if order_by_type not in ["key", "value"]:
            raise ERROR_INVALID_PARAMETER(
                key="options.PIVOT.order_by.type",
                reason=f"Invalid order_by type: {order_by_type}",
            )

    def _apply_order_by(
        self,
        pivot_table: pd.DataFrame,
        order_by: dict,
        column_fields: list,
    ) -> pd.DataFrame:
        order_by_type = order_by.get("type", "key")
        desc = order_by.get("desc", False)
        if order_by_type == "key":
            pivot_table = pivot_table[
                self.label_keys + sorted(column_fields, reverse=desc)
            ]
        else:
            column_sums = pivot_table.drop(columns=self.label_keys).sum()
            if desc:
                ascending = False
            else:
                ascending = True
            sorted_columns = column_sums.sort_values(ascending=ascending).index.tolist()
            pivot_table = pivot_table[self.label_keys + sorted_columns]
        return pivot_table

    @staticmethod
    def _validate_case(case):
        required_keys = ["match", "value", "operator"]
        for key in required_keys:
            if key not in case:
                raise ERROR_REQUIRED_PARAMETER(key=f"options.VALUE_MAPPING.cases.{key}")

    @staticmethod
    def extract_fields_from_condition(condition: str) -> list:
        return re.findall(r"`([^`]+)`", condition)

    def convert_datetime_fields(self, df: pd.DataFrame, conditions: list) -> tuple:
        converted_fields = set()

        for condition in conditions:
            converted_fields.update(self.extract_fields_from_condition(condition))

        for field in converted_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")

        return df, converted_fields

    @staticmethod
    def revert_datetime_to_string(df: pd.DataFrame, fields: set) -> pd.DataFrame:
        for field in fields:
            if field in df.columns:
                df[field] = df[field].astype(str)
        return df

    def apply_query(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        try:
            fields_in_condition = self.extract_fields_from_condition(condition)

            if "Date" in fields_in_condition:
                df, converted_fields = self.convert_datetime_fields(df, [condition])
                df = df.query(condition).copy()
                df = self.revert_datetime_to_string(df, converted_fields)
            else:
                df = df.query(condition).copy()

        except Exception as e:
            _LOGGER.error(f"[apply_query] query error: {e}")
            raise ERROR_INVALID_PARAMETER(key="query.condition", reason=condition)

        return df
