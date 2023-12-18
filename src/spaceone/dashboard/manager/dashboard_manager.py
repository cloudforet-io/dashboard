import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model.dashboard_model import Dashboard

_LOGGER = logging.getLogger(__name__)


class DashboardManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_model: Dashboard = self.locator.get_model("Dashboard")

    def create_dashboard(self, params: dict) -> Dashboard:
        def _rollback(vo: Dashboard) -> None:
            _LOGGER.info(
                f"[create_dashboard._rollback] "
                f"Delete vo : {vo.name} "
                f"({vo.dashboard_id})"
            )
            vo.delete()

        dashboard_vo: Dashboard = self.dashboard_model.create(params)
        self.transaction.add_rollback(_rollback, dashboard_vo)

        return dashboard_vo

    def update_dashboard(self, params: dict) -> Dashboard:
        dashboard_vo: Dashboard = self.get_dashboard(
            params["dashboard_id"], params["domain_id"]
        )
        return self.update_dashboard_by_vo(params, dashboard_vo)

    def update_dashboard_by_vo(
        self, params: dict, dashboard_vo: Dashboard
    ) -> Dashboard:
        def _rollback(old_data: dict) -> None:
            _LOGGER.info(
                f"[update_dashboard_by_vo._rollback] Revert Data : "
                f'{old_data["dashboard_id"]}'
            )
            dashboard_vo.update(old_data)

        self.transaction.add_rollback(_rollback, dashboard_vo.to_dict())
        return dashboard_vo.update(params)

    def delete_dashboard(self, dashboard_id: str, domain_id: str) -> None:
        dashboard_vo: Dashboard = self.get_dashboard(dashboard_id, domain_id)
        dashboard_vo.delete()

    def get_dashboard(
        self,
        dashboard_id: str,
        domain_id: str,
        workspace_id: str = None,
        user_projects=None,
    ) -> Dashboard:
        conditions = {"dashboard_id": dashboard_id, "domain_id": domain_id}

        if workspace_id:
            conditions["workspace_id"] = workspace_id

        if user_projects:
            conditions["project_id"] = user_projects

        return self.dashboard_model.get(**conditions)

    def list_dashboards(self, query=None):
        if query is None:
            query = {}
        return self.dashboard_model.query(**query)

    def stat_dashboards(self, query: dict) -> dict:
        return self.dashboard_model.stat(**query)

    def increase_version(self, dashboard_vo: Dashboard) -> None:
        def _rollback(vo: Dashboard):
            _LOGGER.info(
                f"[increase_version._rollback] Decrease Version : " f"{vo.dashboard_id}"
            )
            vo.decrement("version")

        dashboard_vo.increment("version")
        self.transaction.add_rollback(_rollback, dashboard_vo)

    @staticmethod
    def delete_by_dashboard_vo(dashboard_vo: Dashboard) -> None:
        dashboard_vo.delete()
