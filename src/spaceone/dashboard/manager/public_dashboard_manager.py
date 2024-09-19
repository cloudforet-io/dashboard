import logging
from typing import Tuple, Union
from mongoengine import QuerySet

from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.public_dashboard.database import PublicDashboard
from spaceone.dashboard.manager.public_data_table_manager import PublicDataTableManager
from spaceone.dashboard.manager.public_widget_manager import PublicWidgetManager

_LOGGER = logging.getLogger(__name__)


class PublicDashboardManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_model = PublicDashboard

    def create_public_dashboard(self, params: dict) -> PublicDashboard:
        def _rollback(vo: PublicDashboard) -> None:
            _LOGGER.info(
                f"[create_public_dashboard._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.dashboard_id})"
            )
            vo.delete()

        dashboard_vo: PublicDashboard = self.dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, dashboard_vo)

        return dashboard_vo

    def update_public_dashboard_by_vo(
        self, params: dict, dashboard_vo: PublicDashboard
    ) -> PublicDashboard:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_public_dashboard_by_vo._rollback] Revert Data : "
                f'{old_data["dashboard_id"]}'
            )
            dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, dashboard_vo.to_dict())
        return dashboard_vo.update(params)

    @staticmethod
    def delete_public_dashboard_by_vo(dashboard_vo: PublicDashboard) -> None:
        # Delete child widgets
        pub_widget_mgr = PublicWidgetManager()
        pub_widget_vos = pub_widget_mgr.filter_public_widgets(
            dashboard_id=dashboard_vo.dashboard_id,
            domain_id=dashboard_vo.domain_id,
        )
        _LOGGER.debug(
            f"[delete_public_dashboard_by_vo] delete widget count: {pub_widget_vos.count()}"
        )
        pub_widget_vos.delete()

        # Delete child data tables
        pub_data_table_mgr = PublicDataTableManager()
        pub_data_table_vos = pub_data_table_mgr.filter_public_data_tables(
            dashboard_id=dashboard_vo.dashboard_id,
            domain_id=dashboard_vo.domain_id,
        )
        _LOGGER.debug(
            f"[delete_public_dashboard_by_vo] delete data table count: {pub_data_table_vos.count()}"
        )
        pub_data_table_vos.delete()

        dashboard_vo.delete()

    def get_public_dashboard(
        self,
        dashboard_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects: list = None,
    ) -> PublicDashboard:
        conditions = {
            "dashboard_id": dashboard_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.dashboard_model.get(**conditions)

    def filter_public_dashboards(self, **conditions) -> QuerySet:
        return self.dashboard_model.filter(**conditions)

    def list_public_dashboards(self, query: dict) -> Tuple[QuerySet, int]:
        return self.dashboard_model.query(**query)

    def stat_public_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.stat(**query)
