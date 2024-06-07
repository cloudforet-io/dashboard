import logging
from typing import Union
import pandas as pd

from spaceone.core.manager import BaseManager
from spaceone.dashboard.error.data_table import (
    ERROR_QUERY_OPTION,
    ERROR_NOT_SUPPORTED_QUERY_OPTION,
)

_LOGGER = logging.getLogger(__name__)


class DataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.df: Union[pd.DataFrame, None] = None

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

    def group_by(self, fields: dict, group_by: list = None) -> None:
        if len(self.df) > 0:
            columns = list(fields.keys())
            if group_by:
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
                self.df = self.df.groupby(group_by).agg(agg_options).reset_index()
            else:
                self.df = self.df.agg(agg_options).to_frame().T

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
