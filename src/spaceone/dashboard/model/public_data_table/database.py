from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PublicDataTable(MongoModel):
    data_table_id = StringField(max_length=40, generate_id="public-dt", unique=True)
    name = StringField(max_length=100, default=None, null=True)
    state = StringField(
        max_length=40, default="AVAILABLE", choices=("AVAILABLE", "UNAVAILABLE")
    )
    error_message = StringField(max_length=255, default=None, null=True)
    data_type = StringField(max_length=40, choices=("ADDED", "TRANSFORMED"))
    source_type = StringField(max_length=40, default=None, null=True)
    operator = StringField(max_length=40, default=None, null=True)
    options = DictField(required=True, default=None)
    tags = DictField(default=None)
    labels_info = DictField(default=None)
    data_info = DictField(default=None)
    dashboard_id = StringField(max_length=40)
    widget_id = StringField(max_length=40)
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
            "state",
            "error_message",
            "options",
            "tags",
            "labels_info",
            "data_info",
            "project_id",
            "workspace_id",
        ],
        "minimal_fields": [
            "data_table_id",
            "name",
            "state",
            "data_type",
            "source_type",
            "operator",
            "dashboard_id",
            "widget_id",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "state",
            "data_type",
            "source_type",
            "operator",
            "dashboard_id",
            "widget_id",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
