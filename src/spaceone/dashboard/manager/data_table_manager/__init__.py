import ast
import logging
import re
from typing import Union, Tuple

import pandas as pd
from jinja2 import Environment, meta
from markupsafe import escape
from spaceone.core import cache, utils
from spaceone.core.manager import BaseManager

from spaceone.dashboard.error.data_table import (
    ERROR_QUERY_OPTION,
    ERROR_QUERY_GROUP_BY_OPTION,
    ERROR_EMPTY_DATA_FIELD,
)

_LOGGER = logging.getLogger(__name__)


class DataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_id = None
        self.domain_id = None

        self.df: Union[pd.DataFrame, None] = None
        self.data_keys = None
        self.label_keys = None
        self.jinja_variables = None
        self.jinja_variables_contain_space = []
        self.state = None
        self.error_message = None
        self.currency = "USD"

    def get_data_and_labels_info(self) -> Tuple[dict, dict]:
        raise NotImplementedError()

    def load(
        self,
        granularity: str,
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def load_from_widget(
        self,
        data_table_id: str,
        granularity: str,
        start: str,
        end: str,
        group_by: list = None,
        sort: list = None,
        page: dict = None,
        vars: dict = None,
        column_sum: bool = False,
    ) -> dict:
        query_data = self._prepare_query_data(
            data_table_id, granularity, start, end, group_by, sort, vars
        )

        cache_hash_key = utils.dict_to_hash(query_data)
        response = self._get_cached_response(cache_hash_key)

        if not response:
            self.load(
                granularity,
                start,
                end,
                vars=vars,
            )

            if self.df is not None:
                self._apply_group_by(group_by)
                data_info, labels_info = self.get_data_and_labels_info()

                response = {
                    "results": self.df.copy(deep=True).to_dict(orient="records"),
                }

                if labels_info:
                    response["labels_info"] = labels_info

                if data_info:
                    response["data_info"] = data_info

                if self.data_keys:
                    order = self.label_keys + self.data_keys
                    response["order"] = order

                cache.set(
                    f"dashboard:widget:load:{self.domain_id}:{self.widget_id}:{cache_hash_key}:{self.currency}",
                    response,
                    expire=600,
                )

                self.df = None

        if column_sum:
            return self.response_sum_data_from_widget(response)

        return self.response_data_from_widget(response, sort, page)

    def response_data_from_widget(
        self,
        response,
        sort: list = None,
        page: dict = None,
    ) -> dict:
        data = response["results"]

        total_count = len(data)

        if sort:
            data = self.apply_sort(data, sort)

        if page:
            data = self.apply_page(data, page)

        results = {
            "results": data,
            "total_count": total_count,
        }

        if "labels_info" in response:
            results["labels_info"] = response["labels_info"]
        if "data_info" in response:
            results["data_info"] = response["data_info"]
        if "order" in response:
            results["order"] = response["order"]

        return results

    def response_sum_data_from_widget(self, response: dict) -> dict:
        data = response["results"]
        if self.data_keys:
            sum_data = {
                key: sum(float(row.get(key, 0)) for row in data)
                for key in self.data_keys
            }
        else:
            if data:
                numeric_columns = {
                    key
                    for row in data
                    for key, value in row.items()
                    if isinstance(value, (int, float))
                }
                sum_data = {
                    key: sum(float(row.get(key, 0)) for row in data)
                    for key in numeric_columns
                }
            else:
                keys_to_sum = list(response.get("data_info", {}).keys())
                sum_data = {key: 0 for key in keys_to_sum}

        # results = [{column: sum_value} for column, sum_value in sum_data.items()]
        results = [sum_data]
        return {
            "results": results,
            "total_count": len(results),
        }

    @staticmethod
    def apply_sort(data: list, sort: list) -> list:
        for rule in reversed(sort):
            key = rule["key"]
            reverse = rule.get("desc", False)
            data = sorted(data, key=lambda item: item[key], reverse=reverse)
        return data

    @staticmethod
    def apply_page(data: list, page: dict) -> list:
        if limit := page.get("limit"):
            if limit > 0:
                start = page.get("start", 1)
                if start < 1:
                    start = 1

                start_index = start - 1
                end_index = start_index + limit
                return data[start_index:end_index]

    def response_data(self, sort: list = None, page: dict = None) -> dict:
        total_count = len(self.df)

        if sort:
            self.apply_sort_to_df(sort)

        if page:
            self.apply_page_df(page)

        df = self.df.copy(deep=True)
        data_info, labels_info = self.get_data_and_labels_info()

        self.df = None

        results = {
            "results": df.to_dict(orient="records"),
            "total_count": total_count,
        }

        if labels_info:
            results["labels_info"] = labels_info

        if data_info:
            results["data_info"] = data_info

        if self.data_keys:
            if self.label_keys is None:
                order = self.data_keys
            else:
                order = self.label_keys + self.data_keys

            results["order"] = order

        return results

    def apply_sort_to_df(self, sort: list) -> None:
        if len(self.df) > 0:
            keys = []
            ascendings = []

            for sort_option in sort:
                key = sort_option.get("key")
                ascending = not sort_option.get("desc", False)

                if key:
                    keys.append(key)
                    ascendings.append(ascending)

            try:
                self.df = self.df.sort_values(by=keys, ascending=ascendings)
            except Exception as e:
                _LOGGER.error(f"[_sort] Sort Error: {e}")
                raise ERROR_QUERY_OPTION(key="sort")

    def apply_page_df(self, page: dict) -> None:
        if len(self.df) > 0:
            if limit := page.get("limit"):
                if limit > 0:
                    start = page.get("start", 1)
                    if start < 1:
                        start = 1

                    self.df = self.df.iloc[start - 1 : start + limit - 1]

    def is_jinja_expression(self, expression: str) -> bool:
        if not expression:
            return False

        if isinstance(expression, list):
            expression = str(expression)

        expression = self._sanitize_input(expression)
        expression = escape(expression)

        env = Environment(autoescape=True)

        jinja_pattern = re.compile(r"\{\{\s*(.*?)\s*\}\}")

        modified_expression = re.sub(
            jinja_pattern,
            lambda m: "{{" + m.group(1).replace(" ", "_") + "}}",
            expression,
        )

        jinja_keys_with_space = {}
        if expression != modified_expression:
            keys_with_space = re.findall(jinja_pattern, expression)
            for key in keys_with_space:
                jinja_keys_with_space[key.replace(" ", "_")] = key

        parsed_content = env.parse(modified_expression)
        variables = meta.find_undeclared_variables(parsed_content)

        if variables:
            if jinja_keys_with_space:
                for key, key_with_space in jinja_keys_with_space.items():
                    self.jinja_variables_contain_space.append(
                        {
                            "origin_key": key_with_space,
                            "modified_key": key,
                        }
                    )

            self.jinja_variables = variables

        return bool(variables)

    def change_global_variables(self, expression: str, vars: dict):
        gv_type_map = {}

        if "global" in self.jinja_variables:
            exclude_keys = set(key for key in self.jinja_variables if key != "global")
            expression = expression.replace("global.", "")

            env = Environment(autoescape=True)
            parsed_content = env.parse(expression)
            jinja_variables = meta.find_undeclared_variables(parsed_content)
            global_variables = jinja_variables - exclude_keys

            for global_variable_key in global_variables:
                if vars and global_variable_key in vars:
                    global_variable_value = vars[global_variable_key]
                    gv_type = type(global_variable_value)

                    if isinstance(global_variable_value, str):
                        global_variable_value = self._sanitize_input(
                            global_variable_value
                        )
                        global_variable_value = escape(global_variable_value)

                    elif isinstance(global_variable_value, (int, float, list)):
                        global_variable_value = escape(str(global_variable_value))

                    gv_type_map[global_variable_value] = gv_type

                    expression = expression.replace(
                        global_variable_key, str(global_variable_value)
                    )

        return expression, gv_type_map

    @staticmethod
    def remove_jinja_braces(
        expression: str,
        gv_type_map: dict = None,
    ) -> Union[str, float, list]:
        if not gv_type_map:
            jinja_pattern = re.compile(r"\{\{\s*(.*?)\s*\}\}")
            modified_expression = re.sub(
                jinja_pattern,
                lambda m: "{{" + m.group(1).replace(" ", "_") + "}}",
                expression,
            )
            expression = modified_expression

        while "{{" in expression and "}}" in expression:
            if re.match(r"{{\s*(\w+)\s*}}", expression):
                expression = re.sub(r"{{\s*(\w+)\s*}}", r"\1", expression)
            elif re.match(r"{{\s*(\d+(\.\d+)?)\s*}}", expression):
                result = re.sub(r"{{\s*(\d+(\.\d+)?)\s*}}", r"\1", expression)
                try:
                    expression = float(result)
                except ValueError:
                    expression = eval(result)
            else:
                expression = expression.replace("{{", "").replace("}}", "").strip()

                pattern = r"\[\d+\]$"
                if re.search(pattern, expression):
                    dict_expression, _ = re.subn(pattern, "", expression)
                    index_str = expression.replace(dict_expression, "").strip()
                    index_str = index_str.replace("[", "").replace("]", "")
                    index = int(index_str)
                    expression = ast.literal_eval(dict_expression)[index]
                try:
                    expression = ast.literal_eval(expression)
                except Exception as e:
                    _LOGGER.error(f"Error: {e}")
                    expression = expression
        return expression

    @staticmethod
    def change_expression_data_type(expression: str, gv_type_map: dict) -> str:
        for gv_value, data_type in gv_type_map.items():
            if isinstance(data_type(gv_value), str):
                expression = expression.replace(gv_value, f'"{gv_value}"')

        return expression

    def change_space_variable(self, expression: str) -> str:
        if self.jinja_variables_contain_space:
            for space_variable in self.jinja_variables_contain_space:
                expression = expression.replace(
                    space_variable["modified_key"], f"`{space_variable['origin_key']}`"
                )

        return expression

    def _prepare_query_data(
        self,
        data_table_id: str,
        granularity: str,
        start: str,
        end: str,
        group_by: list,
        sort: list,
        vars: dict,
    ) -> dict:
        user_id = self.transaction.get_meta(
            "authorization.user_id"
        ) or self.transaction.get_meta("authorization.app_id")
        role_type = self.transaction.get_meta("authorization.role_type")

        query_data = {
            "granularity": granularity,
            "start": start,
            "end": end,
            "group_by": group_by,
            "sort": sort,
            "data_table_id": data_table_id,
            "widget_id": self.widget_id,
            "domain_id": self.domain_id,
            "vars": vars,
        }

        if role_type == "WORKSPACE_OWNER":
            workspace_id = self.transaction.get_meta("authorization.workspace_id")
            query_data["vars"]["workspace_id"] = [workspace_id]

        elif role_type == "WORKSPACE_MEMBER":
            query_data["user_id"] = user_id

        return query_data

    def _get_cached_response(self, cache_hash_key) -> dict:
        cache_data = cache.get(
            f"dashboard:widget:load:{self.domain_id}:{self.widget_id}:{cache_hash_key}:{self.currency}"
        )
        return cache_data if cache_data else None

    def _apply_group_by(self, group_by: list):
        if group_by and not self.df.empty:
            for key in group_by:
                if key not in self.df.columns:
                    raise ERROR_QUERY_GROUP_BY_OPTION(
                        key="group_by", fields=list(self.df.columns)
                    )

            agg_funcs = {
                column: "sum"
                for column in self.df.columns
                if pd.api.types.is_numeric_dtype(self.df[column])
            }

            if not agg_funcs:
                raise ERROR_EMPTY_DATA_FIELD(fields=list(self.df.columns))

            self.df = self.df.groupby(group_by).agg(agg_funcs).reset_index()

    @staticmethod
    def _sanitize_input(value: str) -> str:
        return re.sub(r"<.*?>", "", value)
