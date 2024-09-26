from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PublicFolder(MongoModel):
    folder_id = StringField(max_length=40, generate_id="public-folder", unique=True)
    name = StringField(max_length=100)
    tags = DictField(default=None)
    shared = BooleanField(default=False)
    scope = StringField(max_length=40, default=None, null=True)
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
            "tags",
            "shared",
            "scope",
            "project_id",
            "workspace_id",
        ],
        "minimal_fields": [
            "folder_id",
            "name",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
        "change_query_keys": {"user_projects": "project_id"},
        "ordering": ["name"],
        "indexes": [
            "name",
            "shared",
            "scope",
            "resource_group",
            "project_id",
            "workspace_id",
            "domain_id",
        ],
    }
