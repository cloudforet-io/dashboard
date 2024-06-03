from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PublicDashboard(MongoModel):
    dashboard_id = StringField(
        max_length=40, generate_id="public-dash", unique=True
    )
    name = StringField(max_length=100)
    description = StringField(default=None)
    version = StringField(max_length=40, default="2.0")
    layouts = ListField(default=None)
    vars = DictField(default=None)
    options = DictField(default=None)
    variables = DictField(default=None)
    variables_schema = DictField(default=None)
    labels = ListField(StringField())
    tags = DictField(default=None)
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
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
