from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector


class CostAnalysisManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_analysis_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="cost_analysis"
        )

    def analyze_cost(self, params: dict) -> dict:
        return self.cost_analysis_conn.dispatch("Cost.analyze", params)

    def list_data_sources(self, params: dict) -> dict:
        return self.cost_analysis_conn.dispatch("DataSource.list", params)
