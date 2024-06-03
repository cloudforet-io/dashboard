from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PrivateDashboard(MongoModel):
    dashboard_id = StringField(max_length=40, generate_id="private-dash", unique=True)
    description = StringField(default=None)
    name = StringField(max_length=100)
    version = StringField(max_length=40, default="2.0")
    layouts = ListField(default=None)
    vars = DictField(default=None)
    options = DictField(default=None)
    variables = DictField(default=None)
    variables_schema = DictField(default=None)
    labels = ListField(StringField())
    tags = DictField(default=None)
    user_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "description",
            "layouts",
            "vars",
            "options",
            "variables",
            "variables_schema",
            "labels",
            "tags",
        ],
        "minimal_fields": [
            "dashboard_id",
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
