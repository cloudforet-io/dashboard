import logging
import ast
import re
from typing import Union, Literal, Tuple
from jinja2 import Environment, meta
import pandas as pd

from spaceone.core import cache, utils
from spaceone.core.manager import BaseManager
from spaceone.dashboard.error.data_table import (
    ERROR_NO_FIELDS_TO_GLOBAL_VARIABLES,
    ERROR_NOT_GLOBAL_VARIABLE_KEY,
    ERROR_QUERY_OPTION,
)

_LOGGER = logging.getLogger(__name__)
GRANULARITY = Literal["DAILY", "MONTHLY", "YEARLY"]


class DataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_id = None
        self.domain_id = None

        self.df: Union[pd.DataFrame, None] = None
        self.data_keys = None
        self.jinja_variables = None
        self.state = None
        self.error_message = None

    def get_data_and_labels_info(self) -> Tuple[dict, dict]:
        raise NotImplementedError()

    def load(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def load_from_widget(
        self,
        granularity: str,
        start: str,
        end: str,
        sort: list = None,
        page: dict = None,
        vars: dict = None,
        column_sum: bool = False,
    ) -> dict:

        user_id = self.transaction.get_meta(
            "authorization.user_id"
        ) or self.transaction.get_meta("authorization.app_id")
        role_type = self.transaction.get_meta("authorization.role_type")

        query_data = {
            "granularity": granularity,
            "start": start,
            "end": end,
            "sort": sort,
            "widget_id": self.widget_id,
            "domain_id": self.domain_id,
        }

        if role_type == "WORKSPACE_MEMBER":
            query_data["user_id"] = user_id

        query_hash = utils.dict_to_hash(query_data)

        response = {"results": []}
        if cache_data := cache.get(f"dashboard:Widget:load:{query_hash}"):
            response = cache_data

        else:
            self.load(
                granularity,
                start,
                end,
                vars=vars,
            )

            if self.df is not None:
                response = {
                    "results": self.df.copy(deep=True).to_dict(orient="records")
                }
                cache.set(f"dashboard:Widget:load:{query_hash}", response, expire=600)

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

        return {
            "results": data,
            "total_count": total_count,
        }

    def response_sum_data_from_widget(self, response) -> dict:
        data = response["results"]
        if self.data_keys:
            sum_data = {
                key: sum(float(row.get(key, 0)) for row in data)
                for key in self.data_keys
            }
        else:
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

        results = [{column: sum_value} for column, sum_value in sum_data.items()]
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
        self.df = None

        return {
            "results": df.to_dict(orient="records"),
            "total_count": total_count,
        }

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
        env = Environment()

        parsed_content = env.parse(expression)
        variables = meta.find_undeclared_variables(parsed_content)

        if variables:
            self.jinja_variables = variables

        return bool(variables)

    def change_global_variables(self, expression: str, vars: dict):
        gv_type_map = {}
        if "global" in self.jinja_variables:

            if not vars:
                raise ERROR_NO_FIELDS_TO_GLOBAL_VARIABLES(vars=vars)

            exclude_keys = set(key for key in self.jinja_variables if key != "global")
            expression = expression.replace("global.", "")

            env = Environment()

            parsed_content = env.parse(expression)
            jinja_variables = meta.find_undeclared_variables(parsed_content)

            global_variables = jinja_variables - exclude_keys
            for global_variable_key in global_variables:

                if global_variable_key not in vars:
                    raise ERROR_NOT_GLOBAL_VARIABLE_KEY(
                        global_variable_key=global_variable_key
                    )

                global_variable_value = vars[global_variable_key]
                gv_type = type(global_variable_value)

                if isinstance(global_variable_value, int) or isinstance(
                    global_variable_value, float
                ):
                    global_variable_value = str(global_variable_value)

                if isinstance(global_variable_value, list):
                    global_variable_value = str(global_variable_value)

                gv_type_map[global_variable_value] = gv_type

                expression = expression.replace(
                    global_variable_key, global_variable_value
                )

        return expression, gv_type_map

    @staticmethod
    def remove_jinja_braces(expression: str) -> Union[str, float, list]:
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
