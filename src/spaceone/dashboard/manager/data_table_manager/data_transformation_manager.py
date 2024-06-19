import logging
from typing import List, Union, Literal
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
)
from spaceone.dashboard.manager.data_table_manager import DataTableManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.model.public_data_table.database import PublicDataTable
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable

_LOGGER = logging.getLogger(__name__)
GRANULARITY = Literal["DAILY", "MONTHLY", "YEARLY"]


class DataTransformationManager(DataTableManager):
    def __init__(
        self,
        operator: str,
        options: dict,
        data_table_vos: List[Union[PublicDataTable, PrivateDataTable]],
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        if operator not in ["JOIN", "CONCAT", "AGGREGATE", "WHERE", "EVALUATE"]:
            raise ERROR_NOT_SUPPORTED_OPERATOR(operator=operator)

        self.operator = operator
        self.options = options
        self.data_table_vos = data_table_vos

    def load_data_table(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ):
        if self.operator == "JOIN":
            self.join_data_tables(granularity, start, end, vars)
        elif self.operator == "CONCAT":
            self.concat_data_tables()
        elif self.operator == "AGGREGATE":
            self.aggregate_data_table()
        elif self.operator == "WHERE":
            self.where_data_table()
        elif self.operator == "EVALUATE":
            self.evaluate_data_table()

    def join_data_tables(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ):
        how = self.options.get("how", "LEFT")
        if how not in ["LEFT", "RIGHT", "INNER", "OUTER"]:
            raise ERROR_INVALID_PARAMETER(key="options.how", reason="Invalid join type")

        origin_vo = self.data_table_vos[0]
        other_vo = self.data_table_vos[1]
        origin_data_keys = set(origin_vo.data_info.keys())
        other_data_keys = set(other_vo.data_info.keys())
        data_keys = list(origin_data_keys | other_data_keys)
        origin_label_keys = set(origin_vo.labels_info.keys())
        other_label_keys = set(other_vo.labels_info.keys())
        label_keys = list(origin_label_keys | other_label_keys)

        on = list(origin_label_keys & other_label_keys)
        fill_na = {}
        for key in data_keys:
            fill_na[key] = 0

        for key in label_keys:
            fill_na[key] = ""

        origin_dt = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_dt = self._get_data_table(other_vo, granularity, start, end, vars)

        merged_dt = origin_dt.merge(other_dt, left_on=on, right_on=on, how=how.lower())
        merged_dt = merged_dt.fillna(value=fill_na)
        self.df = merged_dt

    @staticmethod
    def _get_data_table(
        data_table_vo: Union[PublicDataTable, PrivateDataTable],
        granularity: GRANULARITY,
        start: str,
        end: str,
        vars: dict,
    ) -> pd.DataFrame:
        if data_table_vo.data_type == "ADDED":
            ds_mgr = DataSourceManager(data_table_vo.source_type, data_table_vo.options)
            return ds_mgr.load_data_source(granularity, start, end, vars)
        else:
            pass

    def concat_data_tables(self):
        pass

    def aggregate_data_table(self):
        pass

    def where_data_table(self):
        pass

    def evaluate_data_table(self):
        pass
