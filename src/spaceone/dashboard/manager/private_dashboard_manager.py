import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.private_dashboard_model import PrivateDashboard

_LOGGER = logging.getLogger(__name__)


class PrivateDashboardManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_model: PrivateDashboard = self.locator.get_model(
            "PrivateDashboard"
        )

    def create_private_dashboard(self, params: dict) -> PrivateDashboard:
        def _rollback(vo: PrivateDashboard) -> None:
            _LOGGER.info(
                f"[create_private_dashboard._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.private_dashboard_id})"
            )
            vo.delete()

        dashboard_vo: PrivateDashboard = self.dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, dashboard_vo)

        return dashboard_vo

    def update_private_dashboard(self, params: dict) -> PrivateDashboard:
        dashboard_vo: PrivateDashboard = self.get_private_dashboard(
            params["private_dashboard_id"], params["workspace_id"], params["domain_id"]
        )
        return self.update_private_dashboard_by_vo(params, dashboard_vo)

    def update_private_dashboard_by_vo(
        self, params: dict, dashboard_vo: PrivateDashboard
    ) -> PrivateDashboard:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_private_dashboard_by_vo._rollback] Revert Data : "
                f'{old_data["private_dashboard_id"]}'
            )
            dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, dashboard_vo.to_dict())
        return dashboard_vo.update(params)

    def delete_private_dashboard(
        self, private_dashboard_id: str, workspace_id: str, domain_id: str
    ) -> None:
        dashboard_vo: PrivateDashboard = self.get_private_dashboard(
            private_dashboard_id, workspace_id, domain_id
        )
        dashboard_vo.delete()

    def get_private_dashboard(
        self, private_dashboard_id: str, workspace_id: str, domain_id: str
    ) -> PrivateDashboard:
        return self.dashboard_model.get(
            private_dashboard_id=private_dashboard_id,
            workspace_id=workspace_id,
            domain_id=domain_id,
        )

    def list_private_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.query(**query)

    def stat_private_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.stat(**query)

    def increase_version(self, dashboard_vo: PrivateDashboard) -> None:
        def _rollback(vo: PrivateDashboard):
            _LOGGER.info(
                f"[increase_version._rollback] Decrease Version : "
                f"{vo.private_dashboard_id}"
            )
            vo.decrement("version")

        dashboard_vo.increment("version")
        self.transaction.add_rollback(_rollback, dashboard_vo)

    @staticmethod
    def delete_by_private_dashboard_vo(dashboard_vo: PrivateDashboard) -> None:
        dashboard_vo.delete()
