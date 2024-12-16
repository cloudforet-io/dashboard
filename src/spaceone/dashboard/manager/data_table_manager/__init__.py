import logging
import ast
import re
from typing import Union, Literal, Tuple
from jinja2 import Environment, meta
import pandas as pd

from spaceone.core import cache
from spaceone.core.manager import BaseManager
from spaceone.dashboard.error.data_table import (
    ERROR_REQUIRED_PARAMETER,
)
from spaceone.dashboard.error.data_table import (
    ERROR_QUERY_OPTION,
    ERROR_NO_FIELDS_TO_GLOBAL_VARIABLES,
    ERROR_NOT_GLOBAL_VARIABLE_KEY,
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
        query: dict,
        vars: dict = None,
        column_sum: bool = False,
    ) -> dict:
        self._check_query(query)
        granularity = query["granularity"]
        start = query["start"]
        end = query["end"]
        sort = query.get("sort")
        page = query.get("page")

        if cache_data := cache.get(
            f"dashboard:Widget:load:{granularity}:{start}:{end}:{vars}:{self.widget_id}:{self.domain_id}"
        ):
            self.df = pd.DataFrame(cache_data)

        else:
            self.load(
                granularity,
                start,
                end,
                vars=vars,
            )

        if column_sum:
            return self.response_sum_data()

        return self.response_data(sort, page)

    def make_cache_data(self, granularity, start, end, vars) -> None:
        cache_key = f"dashboard:Widget:load:{granularity}:{start}:{end}:{vars}:{self.widget_id}:{self.domain_id}"
        if not cache.get(cache_key) and self.df is not None:
            cache.set(
                cache_key,
                self.df.to_dict(orient="records"),
                expire=1800,
            )

    @staticmethod
    def _check_query(query: dict) -> None:
        if "granularity" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.granularity")

        if "start" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.start")

        if "end" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.end")

    def response_data(self, sort: list = None, page: dict = None) -> dict:
        total_count = len(self.df)

        if sort:
            self.apply_sort(sort)

        if page:
            self.apply_page(page)

        df = self.df.copy(deep=True)
        self.df = None

        return {
            "results": df.to_dict(orient="records"),
            "total_count": total_count,
        }

    def response_sum_data(self) -> dict:
        if self.data_keys:
            sum_data = {
                key: (float(self.df[key].sum()))
                for key in self.data_keys
                if key in self.df.columns
            }
        else:
            numeric_columns = self.df.select_dtypes(include=["float", "int"]).columns
            sum_data = {col: float(self.df[col].sum()) for col in numeric_columns}

        results = [{column: sum_value} for column, sum_value in sum_data.items()]

        self.df = None

        return {
            "results": results,
            "total_count": len(results),
        }

    def apply_sort(self, sort: list) -> None:
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

    def apply_page(self, page: dict) -> None:
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
