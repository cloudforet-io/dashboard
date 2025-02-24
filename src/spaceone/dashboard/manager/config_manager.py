from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector


class ConfigManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_config_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="config"
        )

    def get_domain_config(self, params: dict) -> dict:
        return self.domain_config_conn.dispatch("DomainConfig.get", params)
