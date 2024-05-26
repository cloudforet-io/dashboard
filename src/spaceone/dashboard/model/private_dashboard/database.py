from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PrivateDashboard(MongoModel):
    private_dashboard_id = StringField(
        max_length=40, generate_id="private-dash", unique=True
    )
    name = StringField(max_length=100, unique_with=["user_id", "workspace_id", "domain_id"])
    version = StringField(max_length=40, default="2.0")
    layouts = ListField(default=None)
    vars = DictField(default=None)
    settings = DictField(default=None)
    variables = DictField(default=None)
    variables_schema = DictField(default=None)
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
            "vars",
            "settings",
            "variables",
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
            "user_id",
            "workspace_id",
            "domain_id",
        ],
    }
