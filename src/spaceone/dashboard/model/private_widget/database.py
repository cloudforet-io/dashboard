from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PrivateWidget(MongoModel):
    widget_id = StringField(max_length=40, generate_id="private-widget", unique=True)
    name = StringField(max_length=100)
    state = StringField(
        max_length=40, default="CREATING", choices=("CREATING", "ACTIVE", "INACTIVE")
    )
    description = StringField(default=None)
    widget_type = StringField(max_length=40, default="NONE")
    size = StringField(default=None, null=True)
    options = DictField(default=None, null=True)
    tags = DictField(default=None)
    data_table_id = StringField(max_length=40, default=None, null=True)
    dashboard_id = StringField(max_length=40)
    user_id = StringField(max_length=40)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "state",
            "description",
            "widget_type",
            "size",
            "options",
            "tags",
            "data_table_id",
        ],
        "minimal_fields": [
            "widget_id",
            "name",
            "state",
            "widget_type",
            "data_table_id",
            "dashboard_id",
            "user_id",
            "domain_id",
        ],
        "ordering": ["name"],
        "indexes": [
            "name",
            "state",
            "widget_type",
            "data_table_id",
            "dashboard_id",
            "user_id",
            "domain_id",
        ],
    }
