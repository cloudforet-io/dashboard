from mongoengine import *
from spaceone.core.error import ERROR_NOT_UNIQUE

from spaceone.core.model.mongo_model import MongoModel


class PublicDashboard(MongoModel):
    public_dashboard_id = StringField(
        max_length=40, generate_id="public-dash", unique=True
    )
    name = StringField(max_length=100)
    version = IntField(default=1)
    layouts = ListField(default=[])
    variables = DictField(default={})
    settings = DictField(default={})
    variables_schema = DictField(default={})
    labels = ListField(StringField())
    tags = DictField(default={})
    resource_group = StringField(
        max_length=40, choices=("DOMAIN", "WORKSPACE", "PROJECT")
    )
    project_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "layouts",
            "variables",
            "settings",
            "variables_schema",
            "labels",
            "tags",
        ],
        "minimal_fields": [
            "public_dashboard_id",
            "name",
            "version",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "labels",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }

    @classmethod
    def create(cls, data):
        dashboard_vos = cls.filter(
            name=data["name"],
            project_id=data["project_id"],
            workspace_id=data["workspace_id"],
            domain_id=data["domain_id"],
        )

        if dashboard_vos.count() > 0:
            raise ERROR_NOT_UNIQUE(key="name", value=data["name"])
        return super().create(data)

    def update(self, data):
        if "name" in data:
            dashboard_vos = self.filter(
                name=data["name"],
                domain_id=self.domain_id,
                workspace_id=self.workspace_id,
                project_id=self.project_id,
                public_dashboard_id__ne=self.public_dashboard_id,
            )

            if dashboard_vos.count() > 0:
                raise ERROR_NOT_UNIQUE(key="name", value=data["name"])
            else:
                return super().update(data)
        return super().update(data)
