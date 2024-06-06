import logging
from typing import Union
import pandas as pd

from spaceone.core.manager import BaseManager
from spaceone.dashboard.error.data_table import ERROR_QUERY_OPTION

_LOGGER = logging.getLogger(__name__)


class DataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.df: Union[pd.DataFrame, None] = None

    def response(self, sort: list = None, page: dict = None) -> dict:
        total_count = len(self.df)

        if sort:
            self._apply_sort(sort)

        if page:
            self._apply_page(page)

        return {
            "results": self.df.to_dict(orient="records"),
            "total_count": total_count,
        }

    def _apply_sort(self, sort: list) -> None:
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

    def _apply_page(self, page: dict) -> None:
        if len(self.df) > 0:
            if limit := page.get("limit"):
                if limit > 0:
                    start = page.get("start", 1)
                    if start < 1:
                        start = 1

                    self.df = self.df.iloc[start - 1 : start + limit - 1]
