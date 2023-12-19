import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.public_dashboard_model import PublicDashboard

_LOGGER = logging.getLogger(__name__)


class PublicDashboardManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_model: PublicDashboard = self.locator.get_model(
            "PublicDashboard"
        )

    def create_public_dashboard(self, params: dict) -> PublicDashboard:
        def _rollback(vo: PublicDashboard) -> None:
            _LOGGER.info(
                f"[create_public_dashboard._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.public_dashboard_id})"
            )
            vo.delete()

        dashboard_vo: PublicDashboard = self.dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, dashboard_vo)

        return dashboard_vo

    def update_public_dashboard(self, params: dict) -> PublicDashboard:
        dashboard_vo: PublicDashboard = self.get_public_dashboard(
            params["public_dashboard_id"], params["domain_id"]
        )
        return self.update_public_dashboard_by_vo(params, dashboard_vo)

    def update_public_dashboard_by_vo(
        self, params: dict, dashboard_vo: PublicDashboard
    ) -> PublicDashboard:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_public_dashboard_by_vo._rollback] Revert Data : "
                f'{old_data["public_dashboard_id"]}'
            )
            dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, dashboard_vo.to_dict())
        return dashboard_vo.update(params)

    def delete_public_dashboard(self, public_dashboard_id: str, domain_id: str) -> None:
        dashboard_vo: PublicDashboard = self.get_public_dashboard(
            public_dashboard_id, domain_id
        )
        dashboard_vo.delete()

    def get_public_dashboard(
        self,
        public_dashboard_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects=None,
    ) -> PublicDashboard:
        conditions = {
            "public_dashboard_id": public_dashboard_id,
            "domain_id": domain_id,
        }

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.dashboard_model.get(**conditions)

    def list_public_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.query(**query)

    def stat_public_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.stat(**query)

    def increase_version(self, dashboard_vo: PublicDashboard) -> None:
        def _rollback(vo: PublicDashboard):
            _LOGGER.info(
                f"[increase_version._rollback] Decrease Version : "
                f"{vo.public_dashboard_id}"
            )
            vo.decrement("version")

        dashboard_vo.increment("version")
        self.transaction.add_rollback(_rollback, dashboard_vo)

    @staticmethod
    def delete_by_public_dashboard_vo(dashboard_vo: PublicDashboard) -> None:
        dashboard_vo.delete()
