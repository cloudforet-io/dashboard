import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.private_data_table.database import PrivateDataTable

_LOGGER = logging.getLogger(__name__)


class PrivateDataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_table_model = PrivateDataTable

    def create_private_data_table(self, params: dict) -> PrivateDataTable:
        def _rollback(vo: PrivateDataTable) -> None:
            _LOGGER.info(
                f"[create_private_data_table._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.data_table_id})"
            )
            vo.delete()

        data_table_vo: PrivateDataTable = self.data_table_model.create(params)
        self.transaction.add_rollback(_rollback, data_table_vo)

        return data_table_vo

    def update_private_data_table_by_vo(
        self, params: dict, data_table_vo: PrivateDataTable
    ) -> PrivateDataTable:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_private_data_table_by_vo._rollback] Revert Data : "
                f'{old_data["data_table_id"]}'
            )
            data_table_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_table_vo.to_dict())
        return data_table_vo.update(params)

    @staticmethod
    def delete_private_data_table_by_vo(data_table_vo: PrivateDataTable) -> None:
        data_table_vo.delete()

    def get_private_data_table(
        self,
        data_table_id: str,
        domain_id: str,
        user_id: str = None,
    ) -> PrivateDataTable:
        conditions = {
            "data_table_id": data_table_id,
            "domain_id": domain_id,
        }

        if user_id:
            conditions.update({"user_id": user_id})

        return self.data_table_model.get(**conditions)

    def filter_private_data_tables(self, **conditions) -> QuerySet:
        return self.data_table_model.filter(**conditions)

    def list_private_data_tables(self, query: dict) -> Tuple[QuerySet, int]:
        return self.data_table_model.query(**query)

    def stat_private_data_tables(self, query: dict) -> dict:
        return self.data_table_model.stat(**query)
