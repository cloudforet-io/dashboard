import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.public_widget.database import PublicWidget
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager

_LOGGER = logging.getLogger(__name__)


class PublicWidgetManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_model = PublicWidget

    def create_public_widget(self, params: dict) -> PublicWidget:
        def _rollback(vo: PublicWidget) -> None:
            _LOGGER.info(
                f"[create_public_widget._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.widget_id})"
            )
            vo.delete()

        widget_vo: PublicWidget = self.widget_model.create(params)
        self.transaction.add_rollback(_rollback, widget_vo)

        return widget_vo

    def update_public_widget_by_vo(
        self, params: dict, widget_vo: PublicWidget
    ) -> PublicWidget:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_public_widget_by_vo._rollback] Revert Data : "
                f'{old_data["widget_id"]}'
            )
            widget_vo.update(old_data)

        self.transaction.add_rollback(_rollback, widget_vo.to_dict())
        return widget_vo.update(params)

    @staticmethod
    def delete_public_widget_by_vo(widget_vo: PublicWidget) -> None:
        # Delete child data tables
        pub_data_table_mgr = PublicDataTableManager()
        pub_data_table_vos = pub_data_table_mgr.filter_public_data_tables(
            widget_id=widget_vo.widget_id,
            domain_id=widget_vo.domain_id,
        )
        pub_data_table_vos.delete()

        widget_vo.delete()

    def get_public_widget(
        self,
        widget_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects=None,
    ) -> PublicWidget:
        conditions = {
            "widget_id": widget_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.widget_model.get(**conditions)

    def filter_public_widgets(self, **conditions) -> QuerySet:
        return self.widget_model.filter(**conditions)

    def list_public_widgets(self, query: dict) -> Tuple[QuerySet, int]:
        return self.widget_model.query(**query)

    def stat_public_widgets(self, query: dict) -> dict:
        return self.widget_model.stat(**query)
