from mongoengine import *

from spaceone.core.model.mongo_model import MongoModel


class PrivateFolder(MongoModel):
    folder_id = StringField(max_length=40, generate_id="private-folder", unique=True)
    name = StringField(max_length=100)
    tags = DictField(default=None)
    resource_group = StringField(
        max_length=40, choices=("DOMAIN", "WORKSPACE", "PROJECT")
    )
    user_id = StringField(max_length=40)
    workspace_id = StringField(max_length=40, default=None, null=True)
    domain_id = StringField(max_length=40)
    created_at = DateTimeField(auto_now_add=True)
    updated_at = DateTimeField(auto_now=True)

    meta = {
        "updatable_fields": [
            "name",
            "tags",
        ],
        "minimal_fields": [
            "folder_id",
            "name",
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
