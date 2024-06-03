import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.private_folder.database import PrivateFolder

_LOGGER = logging.getLogger(__name__)


class PrivateFolderManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_model = PrivateFolder

    def create_private_folder(self, params: dict) -> PrivateFolder:
        def _rollback(vo: PrivateFolder) -> None:
            _LOGGER.info(
                f"[create_private_folder._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.folder_id})"
            )
            vo.delete()

        folder_vo: PrivateFolder = self.folder_model.create(params)
        self.transaction.add_rollback(_rollback, folder_vo)

        return folder_vo

    def update_private_folder_by_vo(
        self, params: dict, folder_vo: PrivateFolder
    ) -> PrivateFolder:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_private_folder_by_vo._rollback] Revert Data : "
                f'{old_data["folder_id"]}'
            )
            folder_vo.update(old_data)

        self.transaction.add_rollback(_rollback, folder_vo.to_dict())
        return folder_vo.update(params)

    @staticmethod
    def delete_private_folder_by_vo(folder_vo: PrivateFolder) -> None:
        folder_vo.delete()

    def get_private_folder(
        self,
        folder_id: str,
        domain_id: str,
        user_id: str,
    ) -> PrivateFolder:
        conditions = {
            "folder_id": folder_id,
            "domain_id": domain_id,
            "user_id": user_id,
        }

        return self.folder_model.get(**conditions)

    def filter_private_folders(self, **conditions) -> QuerySet:
        return self.folder_model.filter(**conditions)

    def list_private_folders(self, query: dict) -> Tuple[QuerySet, int]:
        return self.folder_model.query(**query)

    def stat_private_folders(self, query: dict) -> dict:
        return self.folder_model.stat(**query)
