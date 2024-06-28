import logging
from typing import Tuple
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.private_widget.database import PrivateWidget
from spaceone.dashboard.manager.private_data_table_manager import (
    PrivateDataTableManager,
)

_LOGGER = logging.getLogger(__name__)


class PrivateWidgetManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.widget_model = PrivateWidget

    def create_private_widget(self, params: dict) -> PrivateWidget:
        def _rollback(vo: PrivateWidget) -> None:
            _LOGGER.info(
                f"[create_private_widget._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.widget_id})"
            )
            vo.delete()

        widget_vo: PrivateWidget = self.widget_model.create(params)
        self.transaction.add_rollback(_rollback, widget_vo)

        return widget_vo

    def update_private_widget_by_vo(
        self, params: dict, widget_vo: PrivateWidget
    ) -> PrivateWidget:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_private_widget_by_vo._rollback] Revert Data : "
                f'{old_data["widget_id"]}'
            )
            widget_vo.update(old_data)

        self.transaction.add_rollback(_rollback, widget_vo.to_dict())
        return widget_vo.update(params)

    @staticmethod
    def delete_private_widget_by_vo(widget_vo: PrivateWidget) -> None:
        # Delete child data tables
        pri_data_table_mgr = PrivateDataTableManager()
        pri_data_table_vos = pri_data_table_mgr.filter_private_data_tables(
            widget_id=widget_vo.widget_id,
            domain_id=widget_vo.domain_id,
        )
        pri_data_table_vos.delete()

        widget_vo.delete()

    def get_private_widget(
        self,
        widget_id: str,
        domain_id: str,
        user_id: str,
    ) -> PrivateWidget:
        conditions = {
            "widget_id": widget_id,
            "domain_id": domain_id,
            "user_id": user_id,
        }

        return self.widget_model.get(**conditions)

    def filter_private_widgets(self, **conditions) -> QuerySet:
        return self.widget_model.filter(**conditions)

    def list_private_widgets(self, query: dict) -> Tuple[QuerySet, int]:
        return self.widget_model.query(**query)

    def stat_private_widgets(self, query: dict) -> dict:
        return self.widget_model.stat(**query)
