import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.public_folder.database import PublicFolder

_LOGGER = logging.getLogger(__name__)


class PublicFolderManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_model = PublicFolder

    def create_public_folder(self, params: dict) -> PublicFolder:
        def _rollback(vo: PublicFolder) -> None:
            _LOGGER.info(
                f"[create_public_folder._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.folder_id})"
            )
            vo.delete()

        folder_vo: PublicFolder = self.folder_model.create(params)
        self.transaction.add_rollback(_rollback, folder_vo)

        return folder_vo

    def update_public_folder_by_vo(
        self, params: dict, folder_vo: PublicFolder
    ) -> PublicFolder:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_public_folder_by_vo._rollback] Revert Data : "
                f'{old_data["folder_id"]}'
            )
            folder_vo.update(old_data)

        self.transaction.add_rollback(_rollback, folder_vo.to_dict())
        return folder_vo.update(params)

    @staticmethod
    def delete_public_folder_by_vo(folder_vo: PublicFolder) -> None:
        folder_vo.delete()

    def get_public_folder(
        self,
        folder_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects=None,
        resource_group=None,
    ) -> PublicFolder:
        conditions = {
            "folder_id": folder_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        if resource_group:
            conditions["resource_group"] = resource_group

        return self.folder_model.get(**conditions)

    def filter_public_folders(self, **conditions) -> QuerySet:
        return self.folder_model.filter(**conditions)

    def list_public_folders(self, query: dict) -> Tuple[QuerySet, int]:
        return self.folder_model.query(**query)

    def stat_public_folders(self, query: dict) -> dict:
        return self.folder_model.stat(**query)
