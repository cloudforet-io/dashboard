from mongoengine import *
from spaceone.core.error import ERROR_NOT_UNIQUE

from spaceone.core.model.mongo_model import MongoModel


class PrivateDashboard(MongoModel):
    private_dashboard_id = StringField(
        max_length=40, generate_id="private-dash", unique=True
    )
    name = StringField(max_length=100)
    version = IntField(default=1)
    layouts = ListField(default=[])
    variables = DictField(default={})
    settings = DictField(default={})
    variables_schema = DictField(default={})
    labels = ListField(StringField())
    tags = DictField(default={})
    user_id = StringField(max_length=40)
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
            "private_dashboard_id",
            "name",
            "version",
            "user_id",
            "workspace_id",
            "domain_id",
        ],
        "ordering": ["name"],
        "indexes": [
            "name",
            "labels",
            "user_id",
            "workspace_id",
            "domain_id",
        ],
    }

    @classmethod
    def create(cls, data):
        dashboard_vos = cls.filter(
            name=data["name"],
            user_id=data["user_id"],
            workspace_id=data["workspace_id"],
            domain_id=data["domain_id"],
        )

        if dashboard_vos.count() > 0:
            raise ERROR_NOT_UNIQUE(key="name", value=data["name"])
        return super().create(data)

    def update(self, data):
        if "name" in data:
            dashboard_vos = self.filter(
                private_dashboard_id__ne=self.private_dashboard_id,
                name=data["name"],
                domain_id=self.domain_id,
                user_id=self.user_id,
            )

            if dashboard_vos.count() > 0:
                raise ERROR_NOT_UNIQUE(key="name", value=data["name"])
            else:
                return super().update(data)
        return super().update(data)
