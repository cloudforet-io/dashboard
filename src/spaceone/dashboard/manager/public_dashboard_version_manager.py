import logging
from spaceone.core.manager import BaseManager
from spaceone.dashboard.model import PublicDashboardVersion
from spaceone.dashboard.model import PublicDashboard

_LOGGER = logging.getLogger(__name__)


class PublicDashboardVersionManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.version_model: PublicDashboardVersion = self.locator.get_model(
            "PublicDashboardVersion"
        )

    def create_version_by_public_dashboard_vo(
        self, dashboard_vo: PublicDashboard, params: dict
    ) -> PublicDashboardVersion:
        def _rollback(vo: PublicDashboardVersion) -> None:
            _LOGGER.info(
                f"[create_public_dashboard_version._rollback] "
                f"Delete public_dashboard_version_vo : {vo.version} "
                f"({vo.public_dashboard_id})"
            )
            vo.delete()

        new_params = {
            "public_dashboard_id": dashboard_vo.public_dashboard_id,
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

        version_vo: PublicDashboardVersion = self.version_model.create(new_params)
        self.transaction.add_rollback(_rollback, version_vo)
        return version_vo

    def delete_version(
        self, public_dashboard_id: str, version: int, domain_id: str
    ) -> None:
        version_vo: PublicDashboardVersion = self.get_version(
            public_dashboard_id, version, domain_id
        )
        version_vo.delete()

    @staticmethod
    def delete_versions_by_public_dashboard_version_vos(public_dashboard_version_vos):
        public_dashboard_version_vos.delete()

    def get_version(self, public_dashboard_id, version, domain_id):
        return self.version_model.get(
            public_dashboard_id=public_dashboard_id,
            version=version,
            domain_id=domain_id,
        )

    def list_versions(self, query: dict = None) -> dict:
        if query is None:
            query = {}
        return self.version_model.query(**query)

    def filter_versions(self, **conditions) -> dict:
        return self.version_model.filter(**conditions)
