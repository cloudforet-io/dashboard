import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.public_data_table.database import PublicDataTable

_LOGGER = logging.getLogger(__name__)


class PublicDataTableManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_table_model = PublicDataTable

    def create_public_data_table(self, params: dict) -> PublicDataTable:
        def _rollback(vo: PublicDataTable) -> None:
            _LOGGER.info(
                f"[create_public_data_table._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.data_table_id})"
            )
            vo.delete()

        data_table_vo: PublicDataTable = self.data_table_model.create(params)
        self.transaction.add_rollback(_rollback, data_table_vo)

        return data_table_vo

    def update_public_data_table_by_vo(
        self, params: dict, data_table_vo: PublicDataTable
    ) -> PublicDataTable:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_public_data_table_by_vo._rollback] Revert Data : "
                f'{old_data["data_table_id"]}'
            )
            data_table_vo.update(old_data)

        self.transaction.add_rollback(_rollback, data_table_vo.to_dict())
        return data_table_vo.update(params)

    @staticmethod
    def delete_public_data_table_by_vo(data_table_vo: PublicDataTable) -> None:
        data_table_vo.delete()

    def get_public_data_table(
        self,
        data_table_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects=None,
    ) -> PublicDataTable:
        conditions = {
            "data_table_id": data_table_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.data_table_model.get(**conditions)

    def filter_public_data_tables(self, **conditions) -> QuerySet:
        return self.data_table_model.filter(**conditions)

    def list_public_data_tables(self, query: dict) -> Tuple[QuerySet, int]:
        return self.data_table_model.query(**query)

    def stat_public_data_tables(self, query: dict) -> dict:
        return self.data_table_model.stat(**query)
