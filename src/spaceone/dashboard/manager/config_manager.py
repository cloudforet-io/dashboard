from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.connector.space_connector import SpaceConnector


class ConfigManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain_config_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="config"
        )

    def get_domain_config(self, params: dict, domain_id: str) -> dict:
        system_token = config.get_global("TOKEN")
        return self.domain_config_conn.dispatch(
            "DomainConfig.get",
            params,
            x_domain_id=domain_id,
            token=system_token,
        )
