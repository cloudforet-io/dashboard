from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector
from spaceone.dashboard.manager.identity_manager import IdentityManager


class CostAnalysisManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identity_mgr = IdentityManager()
        self.cost_analysis_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="cost_analysis"
        )

    def analyze_cost(self, params: dict) -> dict:
        # params["query"] = self._change_filter_project_group_id(
        #     params.get("query", {}), params.get("domain_id")
        # )
        return self.cost_analysis_conn.dispatch("Cost.analyze", params)

    def analyze_unified_cost(self, params: dict) -> dict:
        # params["query"] = self._change_filter_project_group_id(
        #     params.get("query", {}), params.get("domain_id")
        # )
        return self.cost_analysis_conn.dispatch("UnifiedCost.analyze", params)

    def list_data_sources(self, params: dict) -> dict:
        return self.cost_analysis_conn.dispatch("DataSource.list", params)

    def _change_filter_project_group_id(self, query: dict, domain_id: str) -> dict:
        change_filter = []

        for condition in query.get("filter", []):
            key = condition.get("k", condition.get("key"))
            value = condition.get("v", condition.get("value"))
            operator = condition.get("o", condition.get("operator"))

            if key == "project_group_id":
                # if self.identity_mgr is None:
                #     self.identity_mgr: IdentityManager = self.locator.get_manager(
                #         "IdentityManager"
                #     )

                project_groups_info = self.identity_mgr.list_project_groups(
                    {
                        "query": {
                            "only": ["project_group_id"],
                            "filter": [{"k": key, "v": value, "o": operator}],
                        }
                    },
                    domain_id,
                )

                project_group_ids = [
                    project_group_info["project_group_id"]
                    for project_group_info in project_groups_info.get("results", [])
                ]

                project_ids = []

                for project_group_id in project_group_ids:
                    projects_info = self.identity_mgr.get_projects_in_project_group(
                        project_group_id, domain_id
                    )
                    project_ids.extend(
                        [
                            project_info["project_id"]
                            for project_info in projects_info.get("results", [])
                        ]
                    )

                project_ids = list(set(project_ids))
                change_filter.append({"k": "project_id", "v": project_ids, "o": "in"})

            else:
                change_filter.append(condition)

        query["filter"] = change_filter
        return query
