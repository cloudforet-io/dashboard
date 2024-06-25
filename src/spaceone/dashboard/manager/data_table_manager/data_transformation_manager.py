import logging
from typing import List, Union, Tuple
import pandas as pd

from spaceone.dashboard.error.data_table import (
    ERROR_INVALID_PARAMETER,
    ERROR_NOT_SUPPORTED_OPERATOR,
    ERROR_REQUIRED_PARAMETER,
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

        if operator not in ["JOIN", "CONCAT", "AGGREGATE", "QUERY", "EVAL"]:
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

        for key in self.data_keys:
            if key not in data_info:
                data_info[key] = {}

        for key in self.label_keys:
            if key not in labels_info:
                labels_info[key] = {}

        return data_info, labels_info

    def load(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> pd.DataFrame:
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

        return self.df

    def join_data_tables(
        self,
        granularity: GRANULARITY = "DAILY",
        start: str = None,
        end: str = None,
        vars: dict = None,
    ) -> None:
        how = self.options.get("how", "left")
        if how not in ["left", "right", "inner", "outer"]:
            raise ERROR_INVALID_PARAMETER(
                key="options.JOIN.how", reason="Invalid join type"
            )

        origin_vo = self.data_table_vos[0]
        other_vo = self.data_table_vos[1]
        origin_data_keys = set(origin_vo.data_info.keys())
        other_data_keys = set(other_vo.data_info.keys())

        duplicate_keys = list(origin_data_keys & other_data_keys)
        if len(duplicate_keys) > 0:
            raise ERROR_INVALID_PARAMETER(
                key="options.JOIN.data_tables",
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

        origin_df = self._get_data_table(origin_vo, granularity, start, end, vars)
        other_df = self._get_data_table(other_vo, granularity, start, end, vars)

        if len(other_df) == 0:
            if how in ["left", "outer"]:
                self.df = origin_df
            else:
                self.df = other_df
            return
        elif len(origin_df) == 0:
            if how in ["right", "outer"]:
                self.df = other_df
            else:
                self.df = origin_df
            return

        merged_df = origin_df.merge(other_df, left_on=on, right_on=on, how=how)
        merged_df = merged_df.fillna(value=fill_na)
        self.df = merged_df

    def concat_data_tables(
        self,
        granularity: GRANULARITY = "DAILY",
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
        granularity: GRANULARITY = "DAILY",
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

        if len(group_by) > 0:
            self.df = df.groupby(group_by).agg(function).reset_index()
        else:
            self.df = df.agg(function).to_frame().T

    def query_data_table(
        self,
        granularity: GRANULARITY = "DAILY",
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
        granularity: GRANULARITY = "DAILY",
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
            try:
                key, value = expression.split("=")
                self.data_keys = list(set(self.data_keys) | {key})

                df = df.eval(expression)

            except Exception as e:
                _LOGGER.error(f"[evaluate_data_table] eval error: {e}")
                raise ERROR_INVALID_PARAMETER(
                    key="options.EVAL.expressions", reason=expression
                )

        self.df = df

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
