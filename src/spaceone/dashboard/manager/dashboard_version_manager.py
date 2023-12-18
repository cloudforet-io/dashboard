import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model import DashboardVersion
from spaceone.dashboard.model import Dashboard

_LOGGER = logging.getLogger(__name__)


class DashboardVersionManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dashboard_version_model: DashboardVersion = self.locator.get_model(
            "DashboardVersion"
        )

    def create_version_by_dashboard_vo(
        self, dashboard_vo: Dashboard, params: dict
    ) -> DashboardVersion:
        def _rollback(vo: DashboardVersion) -> None:
            _LOGGER.info(
                f"[create_dashboard_version._rollback] "
                f"Delete dashboard_version_vo : {vo.version} "
                f"({vo.dashboard_id})"
            )
            vo.delete()

        new_params = {
            "dashboard_id": dashboard_vo.dashboard_id,
            "version": dashboard_vo.version,
            "layouts": params.get("layouts")
            if params.get("layouts")
            else dashboard_vo.layouts,
            "variables": params.get("variables")
            if params.get("variables")
            else dashboard_vo.variables,
            "settings": params.get("settings")
            if params.get("settings")
            else dashboard_vo.settings,
            "variables_schema": params.get("variables_schema")
            if params.get("variables_schema")
            else dashboard_vo.variables_schema,
            "domain_id": dashboard_vo.domain_id,
        }

        version_vo: DashboardVersion = self.dashboard_version_model.create(new_params)
        self.transaction.add_rollback(_rollback, version_vo)
        return version_vo

    def delete_version(self, dashboard_id: str, version: int, domain_id: str) -> None:
        version_vo: DashboardVersion = self.get_version(
            dashboard_id, version, domain_id
        )
        version_vo.delete()

    @staticmethod
    def delete_versions_by_dashboard_version_vos(domain_dashboard_version_vos):
        domain_dashboard_version_vos.delete()

    def get_version(self, dashboard_id, version, domain_id):
        return self.dashboard_version_model.get(
            dashboard_id=dashboard_id,
            version=version,
            domain_id=domain_id,
        )

    def list_versions(self, query: dict = None) -> dict:
        if query is None:
            query = {}
        return self.dashboard_version_model.query(**query)

    def filter_versions(self, **conditions) -> dict:
        return self.dashboard_version_model.filter(**conditions)
