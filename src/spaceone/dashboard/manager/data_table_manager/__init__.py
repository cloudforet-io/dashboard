import logging
import ast
import re
from typing import Union, Literal, Tuple
from jinja2 import Environment, meta
import pandas as pd

from spaceone.core.manager import BaseManager
from spaceone.dashboard.error.data_table import (
    ERROR_REQUIRED_PARAMETER,
)
from spaceone.dashboard.error.data_table import (
    ERROR_QUERY_OPTION,
    ERROR_NOT_SUPPORTED_QUERY_OPTION,
    ERROR_INVALID_PARAMETER,
    ERROR_NO_FIELDS_TO_GLOBAL_VARIABLES,
    ERROR_NOT_GLOBAL_VARIABLE_KEY,
)

_LOGGER = logging.getLogger(__name__)
GRANULARITY = Literal["DAILY", "MONTHLY", "YEARLY"]


class DataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.df: Union[pd.DataFrame, None] = None
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

    def load_from_widget(self, query: dict, vars: dict = None) -> dict:
        self._check_query(query)
        granularity = query["granularity"]
        start = query["start"]
        end = query["end"]
        group_by = query.get("group_by")
        filter = query.get("filter")
        fields = query.get("fields")
        field_group = query.get("field_group")
        sort = query.get("sort")
        page = query.get("page")

        self.load(
            granularity,
            start,
            end,
            vars=vars,
        )

        if filter:
            self.apply_filter(filter)

        if fields:
            self.apply_group_by(fields, group_by)

        if field_group:
            self.apply_field_group(field_group, fields)

            if sort:
                changed_sort = []
                for condition in sort:
                    key = condition.get("key")
                    desc = condition.get("desc", False)

                    if key in fields:
                        changed_sort.append({"key": f"_total_{key}", "desc": desc})
                    else:
                        changed_sort.append(condition)

                sort = changed_sort

        return self.response(sort, page)

    @staticmethod
    def _check_query(query: dict) -> None:
        if "granularity" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.granularity")

        if "start" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.start")

        if "end" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.end")

        if "fields" not in query:
            raise ERROR_REQUIRED_PARAMETER(key="query.fields")

        if "select" in query:
            raise ERROR_NOT_SUPPORTED_QUERY_OPTION(key="query.select")

        if "filter_or" in query:
            raise ERROR_NOT_SUPPORTED_QUERY_OPTION(key="query.filter_or")

    def response(self, sort: list = None, page: dict = None) -> dict:
        total_count = len(self.df)

        if sort:
            self.apply_sort(sort)

        if page:
            self.apply_page(page)

        return {
            "results": self.df.to_dict(orient="records"),
            "total_count": total_count,
        }

    def apply_filter(self, filter: list) -> None:
        if len(self.df) > 0:
            for condition in filter:
                key = condition.get("key", condition.get("k"))
                operator = condition.get("operator", condition.get("o"))
                value = condition.get("value", condition.get("v"))

                if operator in ["in", "not in"]:
                    if not isinstance(value, list):
                        raise ERROR_QUERY_OPTION(key="filter")

                if key and operator and value:
                    try:
                        if operator == "in":
                            self.df = self.df[self.df[key].isin(value)]
                        elif operator == "not_in":
                            self.df = self.df[~self.df[key].isin(value)]
                        elif operator == "eq":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} == {value}")
                            else:
                                self.df = self.df.query(f"{key} == '{value}'")
                        elif operator == "not":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} != {value}")
                            else:
                                self.df = self.df.query(f"{key} != '{value}'")
                        elif operator == "gt":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} > {value}")
                            else:
                                self.df = self.df.query(f"{key} > '{value}'")
                        elif operator == "gte":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} >= {value}")
                            else:
                                self.df = self.df.query(f"{key} >= '{value}'")
                        elif operator == "lt":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} < {value}")
                            else:
                                self.df = self.df.query(f"{key} < '{value}'")
                        elif operator == "lte":
                            if isinstance(value, int) or isinstance(value, float):
                                self.df = self.df.query(f"{key} <= {value}")
                            else:
                                self.df = self.df.query(f"{key} <= '{value}'")
                        elif operator == "contain":
                            self.df = self.df[self.df[key].str.contains(str(value))]
                        elif operator == "not_contain":
                            self.df = self.df[~self.df[key].str.contains(str(value))]
                        else:
                            raise ERROR_NOT_SUPPORTED_QUERY_OPTION(
                                key=f"filter.operator.{operator}"
                            )
                    except Exception as e:
                        raise ERROR_QUERY_OPTION(key="filter")
                else:
                    raise ERROR_QUERY_OPTION(key="filter")

    def apply_group_by(self, fields: dict, group_by: list = None) -> None:
        if len(self.df) > 0:
            columns = list(fields.keys())
            for key in columns:
                if key not in self.df.columns:
                    self.df[key] = 0

            if group_by:
                group_by = list(set(group_by))
                for key in group_by:
                    if key not in self.df.columns:
                        self.df[key] = ""

                columns.extend(group_by)

            self.df = self.df[columns]

            agg_options = {}
            for name, options in fields.items():
                operator = options.get("operator", "sum")
                if operator not in ["sum", "average", "max", "min"]:
                    raise ERROR_NOT_SUPPORTED_QUERY_OPTION(
                        key=f"fields.operator.{operator}"
                    )

                if operator == "average":
                    operator = "mean"

                agg_options[name] = operator

            if group_by:
                df = self.df.groupby(group_by).agg(agg_options).reset_index()
                if not df.empty:
                    self.df = df

            else:
                self.df = self.df.agg(agg_options).to_frame().T

    def apply_field_group(self, field_group: list, fields: dict) -> None:
        if len(self.df) > 0:
            for key in field_group:
                if key not in self.df.columns:
                    raise ERROR_INVALID_PARAMETER(
                        key="query.field_group", reason=f"Invalid key: {key}"
                    )

            data_fields = list(fields.keys())
            agg_fields = set(data_fields + field_group)
            group_by = list(set(self.df.columns) - agg_fields)
            agg_options = {}
            for field in agg_fields:
                agg_options[field] = lambda x: list(x)

            if group_by:
                self.df = self.df.groupby(group_by).agg(agg_options).reset_index()
                rows = self.df.to_dict(orient="records")
            else:
                aggr_row = {}
                for key in agg_options.keys():
                    aggr_row[key] = []

                for row in self.df.to_dict(orient="records"):
                    for key in agg_options.keys():
                        if key in row:
                            aggr_row[key].append(row[key])

                rows = [aggr_row]

            changed_data = []
            for row in rows:
                changed_row = {}
                for data_field in data_fields:
                    changed_row[data_field] = []

                for key, value in row.items():
                    if key in group_by:
                        changed_row[key] = value
                    elif key in fields:
                        operator = fields[key].get("operator", "sum")
                        if operator == "sum":
                            changed_row[f"_total_{key}"] = sum(value)
                        elif operator == "average":
                            changed_row[f"_total_{key}"] = sum(value) / len(value)
                        elif operator == "max":
                            changed_row[f"_total_{key}"] = max(value)
                        elif operator == "min":
                            changed_row[f"_total_{key}"] = min(value)

                        for idx, v in enumerate(value):
                            data = {"value": v}
                            for fg in field_group:
                                data[fg] = row[fg][idx]

                            changed_row[key].append(data)

                changed_data.append(changed_row)

            self.df = pd.DataFrame(changed_data)

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
        if re.match(r"{{\s*(\w+)\s*}}", expression):
            return re.sub(r"{{\s*(\w+)\s*}}", r"\1", expression)
        elif re.match(r"{{\s*(\d+(\.\d+)?)\s*}}", expression):
            result = re.sub(r"{{\s*(\d+(\.\d+)?)\s*}}", r"\1", expression)
            return float(result)
        else:
            expression = expression.replace("{{", "").replace("}}", "").strip()
            return ast.literal_eval(expression)

    @staticmethod
    def change_expression_data_type(expression: str, gv_type_map: dict) -> str:
        for gv_value, data_type in gv_type_map.items():
            if isinstance(data_type(gv_value), str):
                expression = expression.replace(gv_value, f'"{gv_value}"')

        return expression
