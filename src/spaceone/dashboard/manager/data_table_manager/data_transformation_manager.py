import logging
from typing import List, Union
import pandas as pd

from spaceone.dashboard.manager.data_table_manager import DataTableManager
from spaceone.dashboard.manager.data_table_manager.data_source_manager import (
    DataSourceManager,
)
from spaceone.dashboard.model.public_data_table.database import PublicDataTable
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable

_LOGGER = logging.getLogger(__name__)


class DataTransformationManager(DataTableManager):
    def join_data_tables(
        self,
        options: dict,
        data_table_vos: List[Union[PublicDataTable, PrivateDataTable]],
    ):
        on = options.get("on", "LEFT")
        origin_dt_vo = data_table_vos[0]
        join_dt_vo = data_table_vos[1]

    def concat_data_tables(
        self,
        options: dict,
        data_table_vos: List[Union[PublicDataTable, PrivateDataTable]],
    ):
        pass

    def aggregate_data_table(
        self, options: dict, data_table_vo: Union[PublicDataTable, PrivateDataTable]
    ):
        pass

    def where_data_table(
        self, options: dict, data_table_vo: Union[PublicDataTable, PrivateDataTable]
    ):
        pass

    def evaluate_data_tables(
        self, options: dict, data_table_vo: Union[PublicDataTable, PrivateDataTable]
    ):
        pass
