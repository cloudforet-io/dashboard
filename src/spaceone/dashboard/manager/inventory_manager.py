from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector


class InventoryManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inventory_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="inventory"
        )

    def analyze_metric_data(self, params: dict) -> dict:
        return self.inventory_conn.dispatch("MetricData.analyze", params)

    def list_metrics(self, params: dict) -> dict:
        return self.inventory_conn.dispatch("Metric.list", params)
