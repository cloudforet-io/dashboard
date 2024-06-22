import logging
from typing import List, Union, Literal, Tuple
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
    ERROR_REQUIRED_PARAMETER,
)
from spaceone.dashboard.manager.data_table_manager import DataTableManager
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
GRANULARITY = Literal["DAILY", "MONTHLY", "YEARLY"]


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

        if operator not in ["JOIN", "CONCAT", "AGGREGATE", "WHERE", "EVALUATE"]:
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

        return data_info, labels_info

    def load_data_table(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
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

        return self.df

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

        duplicate_keys = list(origin_data_keys & other_data_keys)
        if len(duplicate_keys) > 0:
            raise ERROR_INVALID_PARAMETER(
                key="data_info",
                reason=f"Duplicate data keys: {', '.join(duplicate_keys)}",
            )

        self.data_keys = list(origin_data_keys | other_data_keys)
        origin_label_keys = set(origin_vo.labels_info.keys())
        other_label_keys = set(other_vo.labels_info.keys())
        self.label_keys = list(origin_label_keys | other_label_keys)

        on = list(origin_label_keys & other_label_keys)
        fill_na = {}
        for key in self.data_keys:
            fill_na[key] = 0

        for key in self.label_keys:
            fill_na[key] = ""

        origin_dt = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_dt = self._get_data_table(other_vo, granularity, start, end, vars)

        merged_dt = origin_dt.merge(other_dt, left_on=on, right_on=on, how=how.lower())
        merged_dt = merged_dt.fillna(value=fill_na)
        self.df = merged_dt

    def concat_data_tables(self):
        pass

    def aggregate_data_table(self):
        pass

    def where_data_table(self):
        pass

    def evaluate_data_table(self):
        pass

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
            return ds_mgr.load_data_source(granularity, start, end, vars)
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
            return ds_mgr.load_data_table(granularity, start, end, vars)

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
