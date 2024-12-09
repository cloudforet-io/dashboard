import logging
from typing import List, Union, Tuple

import numpy as np
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
    ERROR_REQUIRED_PARAMETER,
    ERROR_DUPLICATED_DATA_FIELDS,
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

        if operator not in ["JOIN", "CONCAT", "AGGREGATE", "QUERY", "EVAL", "PIVOT"]:
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
            elif self.operator == "PIVOT":
                self.pivot_data_table(granularity, start, end, vars)

            self.state = "AVAILABLE"
            self.error_message = None

        except Exception as e:
            self.state = "UNAVAILABLE"
            self.error_message = e.message if hasattr(e, "message") else str(e)
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

    def pivot_data_table(
        self,
        granularity: GRANULARITY = "MONTHLY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        origin_vo = self.data_table_vos[0]
        field_options = self.options["fields"]
        label_fields, column_field, data_field = (
            field_options["label_fields"],
            field_options["column_field"],
            field_options["data_field"],
        )
        aggregation = field_options.get("aggregation", "sum")

        raw_df = self._get_data_table(origin_vo, granularity, start, end, vars)
        self._check_columns(raw_df, label_fields, column_field, data_field)
        fill_value = self._set_fill_value_from_df(raw_df, data_field)

        pivot_table = self._create_pivot_table(
            raw_df, label_fields, column_field, data_field, aggregation, fill_value
        )
        pivot_table = self._sort_and_filter_pivot_table(pivot_table, label_fields)
        self.df = pivot_table

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
        # 키 매핑
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
        for label_field in label_fields:
            if label_field not in df_columns:
                raise ERROR_INVALID_PARAMETER(
                    key=f"options.PIVOT.label_fields",
                    reason=f"Invalid key: {label_field}, columns={list(df_columns)}",
                )

        if column_field not in df_columns:
            raise ERROR_INVALID_PARAMETER(
                key=f"options.PIVOT.column_field",
                reason=f"Invalid key: {column_field}, columns={list(df_columns)}",
            )

        if data_field not in df_columns:
            raise ERROR_INVALID_PARAMETER(
                key=f"options.PIVOT.data_field",
                reason=f"Invalid key: {data_field}, columns={list(df_columns)}",
            )

    @staticmethod
    def _set_fill_value_from_df(df: pd.DataFrame, data_field: str) -> Union[int, str]:
        if df[data_field].dtype == "object":
            return ""
        return 0

    @staticmethod
    def _set_new_column_names(pivot_table: pd.DataFrame) -> pd.DataFrame:
        new_columns = [
            lower_col if lower_col else upper_col
            for upper_col, lower_col in pivot_table.columns
        ]
        pivot_table.columns = new_columns
        return pivot_table

    def _set_keys(self, columns: list) -> None:
        self.label_keys = [
            upper_col for upper_col, lower_col in columns if not lower_col
        ]
        self.data_keys = [lower_col for upper_col, lower_col in columns if lower_col]

    @staticmethod
    def _validate_manual_column_fields(
        manual_column_fields: list,
        column_fields: list,
    ) -> None:
        for manual_column_field in manual_column_fields:
            if manual_column_field not in column_fields:
                raise ERROR_INVALID_PARAMETER(
                    key="options.PIVOT.manual_column_fields",
                    reason=f"Invalid key: {manual_column_field}, columns={column_fields}",
                )

    def _create_pivot_table(
        self,
        raw_df: pd.DataFrame,
        label_fields: list,
        column_field: str,
        data_field: str,
        aggregation: str,
        fill_value: Union[int, str],
    ) -> pd.DataFrame:
        try:
            pivot_table = pd.pivot_table(
                raw_df,
                values=[data_field],
                index=label_fields,
                columns=[column_field],
                aggfunc=aggregation,
                fill_value=fill_value,
            )
            pivot_table.reset_index(inplace=True)
            self._set_keys(list(pivot_table.columns))
            return self._set_new_column_names(pivot_table)
        except Exception as e:
            _LOGGER.error(f"[pivot_data_table] pivot error: {e}")
            raise ERROR_INVALID_PARAMETER(key="options.PIVOT", reason=str(e))

    def _sort_and_filter_pivot_table(
        self,
        pivot_table: pd.DataFrame,
        label_fields: list,
    ) -> pd.DataFrame:
        column_field_items = list(set(pivot_table.columns) - set(label_fields))
        if not self.total_series:
            self.total_series = pivot_table[column_field_items].sum(axis=1)

        if sort := self.options.get("sort"):
            pivot_table = self._apply_row_sorting(pivot_table, sort, column_field_items)
            pivot_table = self._apply_column_sorting(pivot_table, sort, label_fields)
            column_field_items = [
                field for field in pivot_table.columns if field not in label_fields
            ]

        if manual_column_fields := self.options.get("manual_column_fields"):
            pivot_table = self._apply_manual_column_sorting(
                pivot_table, manual_column_fields, label_fields, column_field_items
            )

        if limit := self.options.get("limit"):
            pivot_table = self._apply_limits(pivot_table, limit, label_fields)

        return pivot_table

    def _apply_row_sorting(
        self,
        pivot_table: pd.DataFrame,
        sort: dict,
        column_field_items: list,
    ) -> pd.DataFrame:
        if sort_keys := sort.get("rows"):
            pivot_table["_total"] = self.total_series

            if len(sort_keys) == 1 and "key" not in sort_keys[0]:
                order_state = self._get_sort_order(sort_keys[0]["order"])
                pivot_table = pivot_table.sort_values(
                    by="_total", ascending=order_state
                )
            else:
                multi_keys, multi_orders = self._parse_sort_keys(
                    sort_keys, column_field_items
                )
                pivot_table = pivot_table.sort_values(
                    by=multi_keys, ascending=multi_orders
                )

            pivot_table = pivot_table.drop(columns=["_total"])
        return pivot_table

    @staticmethod
    def _apply_column_sorting(
        pivot_table: pd.DataFrame,
        sort: dict,
        label_fields: list,
    ) -> pd.DataFrame:
        if sort.get("column_fields"):
            column_sums = pivot_table.drop(columns=label_fields).sum()
            sorted_columns = column_sums.sort_values(ascending=False).index.tolist()
            pivot_table = pivot_table[label_fields + sorted_columns]
        return pivot_table

    def _apply_manual_column_sorting(
        self,
        pivot_table: pd.DataFrame,
        manual_column_fields: list,
        label_fields: list,
        column_field_items: list,
    ) -> pd.DataFrame:
        self._validate_manual_column_fields(manual_column_fields, column_field_items)
        sorted_columns = []
        for column_field in column_field_items:
            if column_field in manual_column_fields:
                sorted_columns.append(column_field)
        pivot_table = pivot_table[label_fields + sorted_columns]
        return pivot_table

    @staticmethod
    def _apply_limits(
        pivot_table: pd.DataFrame,
        limit: dict,
        label_fields: list,
    ) -> pd.DataFrame:
        if rows := limit.get("rows"):
            pivot_table = pivot_table.head(rows)
        if column_fields := limit.get("column_fields"):
            pivot_table = pivot_table.iloc[:, : len(label_fields) + column_fields]
        return pivot_table

    def _parse_sort_keys(self, sort_keys: list, column_field_items: list) -> tuple:
        multi_keys = []
        multi_orders = []
        for sort_key_info in sort_keys:
            sort_key = sort_key_info.get("key")
            sort_order = sort_key_info.get("order")

            if sort_key != "_total" and sort_key not in column_field_items:
                raise ERROR_INVALID_PARAMETER(
                    key="options.PIVOT.sort.keys",
                    reason=f"Invalid key: {sort_key}, columns={column_field_items}",
                )

            order_state = self._get_sort_order(sort_order)
            multi_keys.append(sort_key)
            multi_orders.append(order_state)
        return multi_keys, multi_orders

    @staticmethod
    def _get_sort_order(sort_order: str) -> bool:
        return sort_order != "desc"
