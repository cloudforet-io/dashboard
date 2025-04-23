from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.core.auth.jwt.jwt_util import JWTUtil
from spaceone.core.connector.space_connector import SpaceConnector


class IdentityManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        token = self.transaction.get_meta("token") or kwargs.get("token")
        self.token_type = JWTUtil.get_value_from_token(token, "typ")
        self.identity_conn: SpaceConnector = self.locator.get_connector(
            "SpaceConnector", service="identity"
        )

    def check_workspace(self, workspace_id: str, domain_id: str) -> None:
        system_token = config.get_global("TOKEN")

        return self.identity_conn.dispatch(
            "Workspace.check",
            {"workspace_id": workspace_id, "domain_id": domain_id},
            token=system_token,
        )

    def check_project_group(self, project_group_id: str) -> None:
        self.identity_conn.dispatch(
            "ProjectGroup.get", {"project_group_id": project_group_id}
        )

    def get_project(self, project_id: str) -> dict:
        return self.identity_conn.dispatch("Project.get", {"project_id": project_id})

    def list_service_accounts(self, workspace_id: str) -> dict:
        if workspace_id:
            query = {"workspace_id": workspace_id}
        else:
            query = {}
        return self.identity_conn.dispatch("ServiceAccount.list", query)

    def list_project_groups(self, params: dict, domain_id: str) -> dict:
        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "ProjectGroup.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("ProjectGroup.list", params)

    def get_projects_in_project_group(self, project_group_id: str, domain_id: str):
        params = {
            "query": {
                "only": ["project_id"],
            },
            "project_group_id": project_group_id,
            "include_children": True,
        }

        if self.token_type == "SYSTEM_TOKEN":
            return self.identity_conn.dispatch(
                "Project.list", params, x_domain_id=domain_id
            )
        else:
            return self.identity_conn.dispatch("Project.list", params)
